package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.sql;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

/**
 * Serializes RDF statements directly from DuckDB result tables to N-Triples or N-Quads files using
 * DuckDB's native {@code COPY TO} statement, completely bypassing Java {@code Statement} object
 * allocation.
 *
 * <p>This class builds a SQL query that performs all RDF serialization logic (term formatting,
 * escaping, angle bracket wrapping) entirely within DuckDB's query engine, then writes the result
 * directly to an output file via {@code COPY TO}. DuckDB's native C++ serializer handles the file
 * I/O.
 *
 * <p>The input is a SQL subquery or table name that produces the following columns:
 *
 * <ul>
 *   <li>{@code subject} (String) — the subject IRI or blank node identifier
 *   <li>{@code predicate} (String) — the predicate IRI
 *   <li>{@code object} (String) — the object value (IRI, literal, or blank node identifier)
 *   <li>{@code object_type} (String) — {@code 'IRI'}, {@code 'LITERAL'}, or {@code 'BNODE'}
 *   <li>{@code object_lang} (String, nullable) — language tag for language-tagged literals
 *   <li>{@code object_datatype} (String, nullable) — datatype IRI for typed literals
 *   <li>{@code graph} (String, nullable) — named graph IRI (used only for N-Quads)
 * </ul>
 *
 * <p>Blank node subjects are identified by the {@code _:} prefix in the {@code subject} column
 * value. Subject IRIs must not start with {@code _:}, as the serializer uses a prefix heuristic to
 * distinguish blank nodes from IRIs. Blank node objects are identified via the {@code object_type}
 * column ({@code 'BNODE'}).
 *
 * <p>Literal values are escaped per the N-Triples specification: backslash, double quote, newline,
 * tab, and carriage return. Other control characters (U+0000-U+0008, U+000B, U+000C,
 * U+000E-U+001F) are not currently escaped to {@code &#92;uXXXX} form. These characters are rare in
 * typical RDF data and will be addressed in a future version if needed.
 *
 * <p>The output file is written in UTF-8 encoding (DuckDB's default for COPY TO), which matches
 * the N-Triples specification requirement.
 *
 * <p><strong>Row count:</strong> The returned row count is obtained via a separate
 * {@code SELECT COUNT(*)} query before the {@code COPY TO} execution, since DuckDB JDBC does not
 * return affected row counts for {@code COPY TO}. This means the source query is executed twice.
 *
 * <p><strong>Thread safety:</strong> This class is stateless and all methods are static. However,
 * the provided {@link Connection} must not be shared with concurrent callers, as DuckDB JDBC
 * connections are single-threaded.
 *
 * @see <a href="https://www.w3.org/TR/n-triples/">W3C N-Triples specification</a>
 * @see <a href="https://www.w3.org/TR/n-quads/">W3C N-Quads specification</a>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DuckDbDirectSerializer {

    /**
     * Column name for the subject IRI or blank node identifier. Blank node subjects must use the
     * {@code _:} prefix (e.g., {@code _:b0}). IRI subjects must not start with {@code _:}.
     */
    public static final String SUBJECT_COL = "subject";

    /** Column name for the predicate IRI. */
    public static final String PREDICATE_COL = "predicate";

    /** Column name for the object value. */
    public static final String OBJECT_COL = "object";

    /** Column name for the object type ({@code 'IRI'}, {@code 'LITERAL'}, or {@code 'BNODE'}). */
    public static final String OBJECT_TYPE_COL = "object_type";

    /** Column name for the language tag (nullable). */
    public static final String OBJECT_LANG_COL = "object_lang";

    /** Column name for the datatype IRI (nullable). */
    public static final String OBJECT_DATATYPE_COL = "object_datatype";

    /** Column name for the named graph IRI (nullable, used only for N-Quads). */
    public static final String GRAPH_COL = "graph";

    static final String FORMAT_NTRIPLES = "nt";

    static final String FORMAT_NQUADS = "nq";

    private static final String OBJECT_TYPE_IRI = "IRI";

    private static final String OBJECT_TYPE_BNODE = "BNODE";

    private static final DSLContext CTX = DSL.using(SQLDialect.DUCKDB);

    /**
     * Serializes RDF data from a DuckDB result query directly to an output file in N-Triples or
     * N-Quads format using DuckDB's {@code COPY TO} statement.
     *
     * <p>The method first counts the number of rows to be written, then executes a {@code COPY TO}
     * statement that performs all string concatenation and escaping within DuckDB's query engine.
     *
     * @param duckdbConn the DuckDB JDBC connection
     * @param resultQuery SQL query or table name producing the expected RDF columns
     * @param rdfFormat the RDF format — must be {@code "nt"} (N-Triples) or {@code "nq"} (N-Quads)
     * @param outputPath the output file path
     * @return the number of statements written
     * @throws IllegalArgumentException if {@code rdfFormat} is not {@code "nt"} or {@code "nq"}
     * @throws NullPointerException if any parameter is {@code null}
     * @throws DuckDbDirectSerializationException if the DuckDB query fails
     */
    @SuppressWarnings("java:S2077") // SQL is constructed programmatically, not from user input
    public static long serialize(Connection duckdbConn, String resultQuery, String rdfFormat, Path outputPath) {
        Objects.requireNonNull(duckdbConn, "duckdbConn must not be null");
        Objects.requireNonNull(resultQuery, "resultQuery must not be null");
        Objects.requireNonNull(rdfFormat, "rdfFormat must not be null");
        Objects.requireNonNull(outputPath, "outputPath must not be null");

        validateFormat(rdfFormat);

        var isNQuads = FORMAT_NQUADS.equals(rdfFormat);
        var innerSelect = buildSerializationSelect(resultQuery, isNQuads);
        var copyToSql = buildCopyToSql(innerSelect, outputPath);

        LOG.debug("Executing DuckDB COPY TO for {} output to [{}]", rdfFormat, outputPath);

        var rowCount = countRows(duckdbConn, resultQuery);

        executeCopyTo(duckdbConn, copyToSql);

        LOG.debug("Wrote {} statements to [{}]", rowCount, outputPath);
        return rowCount;
    }

    /**
     * Builds the SQL query that produces a visible version of the internal serialization SELECT,
     * useful for debugging and testing.
     *
     * @param resultQuery SQL query or table name producing the expected RDF columns
     * @param rdfFormat the RDF format — must be {@code "nt"} or {@code "nq"}
     * @return the serialization SELECT SQL string
     */
    static String buildSerializationSql(String resultQuery, String rdfFormat) {
        var isNQuads = FORMAT_NQUADS.equals(rdfFormat);
        return buildSerializationSelect(resultQuery, isNQuads);
    }

    // --- Validation ---

    private static void validateFormat(String rdfFormat) {
        if (!FORMAT_NTRIPLES.equals(rdfFormat) && !FORMAT_NQUADS.equals(rdfFormat)) {
            throw new IllegalArgumentException(
                    "Unsupported RDF format for direct serialization: '%s'. Only 'nt' (N-Triples) and 'nq' (N-Quads) are supported."
                            .formatted(rdfFormat));
        }
    }

    // --- SQL construction ---

    /**
     * Builds the inner SELECT that performs NT/NQ string concatenation in SQL. Each row produces a
     * single text column containing one N-Triples or N-Quads line.
     *
     * <p>The SQL uses CASE expressions to handle different term types (IRI, BNODE, LITERAL) and
     * nested REPLACE chains for N-Triples escaping.
     */
    private static String buildSerializationSelect(String resultQuery, boolean isNQuads) {
        var subjectExpr = buildSubjectExpression();
        var predicateExpr = buildPredicateExpression();
        var objectExpr = buildObjectExpression();

        String lineExpr;
        if (isNQuads) {
            var graphExpr = buildGraphExpression();
            // <subject> <predicate> <object> <graph> .
            // When graph is NULL, omit the graph part (produces N-Triple line within N-Quads file)
            lineExpr = "%s || ' ' || %s || ' ' || %s || CASE WHEN %s IS NOT NULL THEN ' ' || %s ELSE '' END || ' .'"
                    .formatted(subjectExpr, predicateExpr, objectExpr, quotedCol(GRAPH_COL), graphExpr);
        } else {
            // <subject> <predicate> <object> .
            lineExpr = "%s || ' ' || %s || ' ' || %s || ' .'".formatted(subjectExpr, predicateExpr, objectExpr);
        }

        return CTX.select(field(sql(lineExpr)).as(name("line")))
                .from(sql("(%s)".formatted(resultQuery)))
                .getSQL();
    }

    /**
     * Builds the subject term expression. Subjects can be IRIs ({@code <iri>}) or blank nodes
     * ({@code _:id}).
     */
    private static String buildSubjectExpression() {
        // Subjects are either IRIs or blank nodes. Use a simple heuristic:
        // if it starts with '_:', it's a blank node — no angle brackets.
        return "CASE WHEN %s LIKE '_:%%' THEN %s ELSE '<' || %s || '>' END"
                .formatted(quotedCol(SUBJECT_COL), quotedCol(SUBJECT_COL), quotedCol(SUBJECT_COL));
    }

    /**
     * Builds the predicate term expression. Predicates are always IRIs.
     */
    private static String buildPredicateExpression() {
        return "'<' || %s || '>'".formatted(quotedCol(PREDICATE_COL));
    }

    /**
     * Builds the object term expression. Objects can be IRIs, blank nodes, or literals (plain,
     * language-tagged, or datatype-annotated). Literal values are escaped per the N-Triples spec.
     */
    private static String buildObjectExpression() {
        var escapedObject = buildEscapeChain(quotedCol(OBJECT_COL));

        return """
                CASE \
                WHEN %s = '%s' THEN '<' || %s || '>' \
                WHEN %s = '%s' THEN %s \
                WHEN %s IS NOT NULL THEN '"' || %s || '"@' || %s \
                WHEN %s IS NOT NULL THEN '"' || %s || '"^^<' || %s || '>' \
                ELSE '"' || %s || '"' \
                END""".formatted(
                        quotedCol(OBJECT_TYPE_COL),
                        OBJECT_TYPE_IRI,
                        quotedCol(OBJECT_COL),
                        quotedCol(OBJECT_TYPE_COL),
                        OBJECT_TYPE_BNODE,
                        quotedCol(OBJECT_COL),
                        quotedCol(OBJECT_LANG_COL),
                        escapedObject,
                        quotedCol(OBJECT_LANG_COL),
                        quotedCol(OBJECT_DATATYPE_COL),
                        escapedObject,
                        quotedCol(OBJECT_DATATYPE_COL),
                        escapedObject);
    }

    /**
     * Builds the graph term expression for N-Quads. Graphs are always IRIs when present.
     */
    private static String buildGraphExpression() {
        return "'<' || %s || '>'".formatted(quotedCol(GRAPH_COL));
    }

    /**
     * Builds a chain of REPLACE calls that escape literal values per the N-Triples specification.
     * The escaping order matters: backslash must be escaped first to avoid double-escaping.
     *
     * <p>Escaped characters:
     * <ul>
     *   <li>{@code \} → {@code \\}</li>
     *   <li>{@code "} → {@code \"}</li>
     *   <li>newline → {@code \n}</li>
     *   <li>carriage return → {@code \r}</li>
     *   <li>tab → {@code \t}</li>
     * </ul>
     *
     * @param columnRef the SQL column reference to escape
     * @return the nested REPLACE expression
     */
    private static String buildEscapeChain(String columnRef) {
        // Order matters: backslash first to avoid double-escaping
        // Use CHR() for special characters to avoid SQL string escaping issues
        return "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(%s, '\\', '\\\\'), '\"', '\\\"'), CHR(10), '\\n'), CHR(13), '\\r'), CHR(9), '\\t')"
                .formatted(columnRef);
    }

    /**
     * Wraps the inner SELECT in a COPY TO statement for DuckDB.
     *
     * <p>Uses {@code FORMAT CSV, HEADER FALSE, DELIMITER '', QUOTE ''} to write raw text lines
     * without any CSV formatting artifacts.
     */
    private static String buildCopyToSql(String innerSelect, Path outputPath) {
        var absolutePath = outputPath.toAbsolutePath().toString();
        return "COPY (%s) TO %s (FORMAT CSV, HEADER FALSE, DELIMITER '', QUOTE '')"
                .formatted(innerSelect, inline(absolutePath));
    }

    // --- Execution ---

    private static long countRows(Connection duckdbConn, String resultQuery) {
        var countSql =
                CTX.selectCount().from(sql("(%s)".formatted(resultQuery))).getSQL();

        try (var statement = duckdbConn.createStatement();
                var resultSet = statement.executeQuery(countSql)) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new DuckDbDirectSerializationException("Failed to count rows for direct serialization", e);
        }
    }

    private static void executeCopyTo(Connection duckdbConn, String copyToSql) {
        try (var statement = duckdbConn.createStatement()) {
            statement.execute(copyToSql);
        } catch (SQLException e) {
            throw new DuckDbDirectSerializationException(
                    "Failed to execute DuckDB COPY TO: %s".formatted(e.getMessage()), e);
        }
    }

    /**
     * Returns a quoted column reference for use in SQL expressions.
     */
    private static String quotedCol(String columnName) {
        return CTX.render(field(name(columnName)));
    }
}
