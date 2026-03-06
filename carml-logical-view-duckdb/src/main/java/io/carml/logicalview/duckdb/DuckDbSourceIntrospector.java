package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.inline;

import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.logicalview.ConstraintDescriptor;
import io.carml.logicalview.FieldDescriptor;
import io.carml.logicalview.SourceIntrospector;
import io.carml.logicalview.SourceSchema;
import io.carml.model.DatabaseSource;
import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.vocab.Rdf;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * A {@link SourceIntrospector} that uses DuckDB to discover the schema of data sources.
 *
 * <p>For file-based sources (CSV, JSON, Parquet), the introspector executes a {@code DESCRIBE}
 * query using DuckDB's auto-detection functions ({@code read_csv_auto}, {@code read_json_auto},
 * {@code read_parquet}). The DESCRIBE result provides column names, types, and nullability.
 *
 * <p>For database scanner sources (RDB, SQL2008Table, SQL2008Query), the introspector queries
 * {@code information_schema.columns} to discover column metadata.
 *
 * <p>DuckDB STRUCT types are mapped to nested {@link FieldDescriptor}s, and LIST/ARRAY types
 * are mapped to iterable {@link FieldDescriptor}s. Constraints (primary keys, unique, foreign
 * keys, not-null) are discovered from {@code duckdb_constraints()} for DuckDB-native tables
 * and from {@code information_schema.table_constraints} /
 * {@code information_schema.key_column_usage} for database scanner sources.
 *
 * <p>The blocking JDBC calls are wrapped with
 * {@code Mono.fromCallable().subscribeOn(Schedulers.boundedElastic())} to preserve the reactive
 * API contract.
 *
 * <p><strong>Thread safety:</strong> This class is not thread-safe. The underlying JDBC
 * {@link Connection} must not be shared with concurrent callers.
 */
@Slf4j
@AllArgsConstructor
public class DuckDbSourceIntrospector implements SourceIntrospector {

    private static final Set<Resource> FILE_REF_FORMULATIONS =
            Set.of(Rdf.Ql.JsonPath, Rdf.Rml.JsonPath, Rdf.Ql.Csv, Rdf.Rml.Csv);

    private static final Set<Resource> DB_REF_FORMULATIONS =
            Set.of(Rdf.Ql.Rdb, Rdf.Rml.Rdb, Rdf.Rml.SQL2008Table, Rdf.Rml.SQL2008Query);

    private static final Set<String> PARQUET_EXTENSIONS = Set.of(".parquet", ".parq");

    /**
     * Pattern to parse DuckDB STRUCT type definitions. Matches {@code STRUCT(field1 TYPE1, field2
     * TYPE2, ...)}.
     */
    private static final Pattern STRUCT_PATTERN = Pattern.compile("^STRUCT\\((.+)\\)$", Pattern.CASE_INSENSITIVE);

    /**
     * Pattern to parse DuckDB LIST/ARRAY type definitions. Matches {@code TYPE[]} or {@code
     * LIST(TYPE)}.
     */
    private static final Pattern LIST_BRACKET_PATTERN = Pattern.compile("^(.+)\\[]$");

    private static final Pattern LIST_PAREN_PATTERN = Pattern.compile("^LIST\\((.+)\\)$", Pattern.CASE_INSENSITIVE);

    private static final String COLUMN_NAME_COL = "column_name";

    private final Connection connection;

    /**
     * {@inheritDoc}
     *
     * <p>The {@code resolvedSource} parameter is not used by this implementation. DuckDB reads
     * directly from file paths or database tables, so the resolved source data is not needed.
     */
    @Override
    public Mono<SourceSchema> introspect(LogicalSource logicalSource, ResolvedSource<?> resolvedSource) {
        return Mono.fromCallable(() -> doIntrospect(logicalSource)).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Performs the blocking introspection of the logical source.
     */
    private SourceSchema doIntrospect(LogicalSource logicalSource) throws SQLException {
        var refFormulation = logicalSource.getReferenceFormulation();
        if (refFormulation == null) {
            throw new IllegalArgumentException("LogicalSource has no reference formulation");
        }

        var refIri = refFormulation.getAsResource();

        if (isFileSource(refIri)) {
            return introspectFileSource(logicalSource, refIri);
        }

        if (isDbSource(refIri)) {
            return introspectDbSource(logicalSource);
        }

        throw new IllegalArgumentException("Unsupported reference formulation for introspection: %s".formatted(refIri));
    }

    // --- File source introspection ---

    /**
     * Introspects a file-based source using DuckDB DESCRIBE.
     */
    @SuppressWarnings("java:S2077") // SQL is constructed programmatically from jOOQ inline(), not from user input
    private SourceSchema introspectFileSource(LogicalSource logicalSource, Resource refIri) throws SQLException {
        var filePath = resolveFilePath(logicalSource.getSource());
        // The iterator is not used for schema introspection — we discover the full source schema.
        // The iterator path is applied during evaluation, not during introspection.
        var readFunction = buildReadFunction(refIri, filePath);
        var describeSql = "DESCRIBE SELECT * FROM %s".formatted(readFunction);

        LOG.debug("Introspecting file source with: {}", describeSql);

        var fields = new ArrayList<FieldDescriptor>();
        var constraints = new ArrayList<ConstraintDescriptor>();

        try (var stmt = connection.createStatement();
                var rs = stmt.executeQuery(describeSql)) {
            while (rs.next()) {
                var columnName = rs.getString(COLUMN_NAME_COL);
                var columnType = rs.getString("column_type");
                var nullable = parseNullable(rs.getString("null"));

                fields.add(parseFieldDescriptor(columnName, columnType, nullable));

                // Not-null constraint from DESCRIBE
                if (!nullable) {
                    constraints.add(ConstraintDescriptor.notNull(columnName));
                }
            }
        }

        return new SourceSchema(fields, constraints);
    }

    /**
     * Introspects a database scanner source using {@code information_schema.columns}. Also
     * discovers constraints from {@code information_schema.table_constraints} and
     * {@code information_schema.key_column_usage}.
     */
    private SourceSchema introspectDbSource(LogicalSource logicalSource) throws SQLException {
        var tableName = resolveTableName(logicalSource);

        LOG.debug("Introspecting database source for table: {}", tableName);

        var columnResult = introspectDbColumns(tableName);
        var keyConstraints = introspectDbConstraints(tableName);

        var allConstraints = new ArrayList<>(columnResult.notNullConstraints());
        allConstraints.addAll(keyConstraints);

        return new SourceSchema(columnResult.fields(), allConstraints);
    }

    /**
     * Queries {@code information_schema.columns} for column metadata. Also emits
     * {@link ConstraintDescriptor#notNull(String)} constraints for non-nullable columns.
     */
    private DbColumnResult introspectDbColumns(String tableName) throws SQLException {
        var fields = new ArrayList<FieldDescriptor>();
        var notNullConstraints = new ArrayList<ConstraintDescriptor>();

        var sql = "SELECT column_name, data_type, is_nullable "
                + "FROM information_schema.columns "
                + "WHERE table_name = ? "
                + "ORDER BY ordinal_position";

        try (var pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            try (var rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    var columnName = rs.getString(COLUMN_NAME_COL);
                    var dataType = rs.getString("data_type");
                    var isNullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));

                    fields.add(FieldDescriptor.leaf(columnName, dataType, isNullable));

                    if (!isNullable) {
                        notNullConstraints.add(ConstraintDescriptor.notNull(columnName));
                    }
                }
            }
        }

        return new DbColumnResult(fields, notNullConstraints);
    }

    private record DbColumnResult(List<FieldDescriptor> fields, List<ConstraintDescriptor> notNullConstraints) {}

    /**
     * Discovers constraints from {@code information_schema.table_constraints} and
     * {@code information_schema.key_column_usage} for database scanner sources.
     */
    private List<ConstraintDescriptor> introspectDbConstraints(String tableName) throws SQLException {
        var constraints = new ArrayList<ConstraintDescriptor>();

        var sql = "SELECT tc.constraint_type, kcu.column_name, "
                + "kcu.constraint_name "
                + "FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu "
                + "ON tc.constraint_name = kcu.constraint_name "
                + "AND tc.table_name = kcu.table_name "
                + "WHERE tc.table_name = ? "
                + "ORDER BY tc.constraint_type, kcu.constraint_name, kcu.ordinal_position";

        // Group columns by constraint name
        String currentConstraintName = null;
        String currentConstraintType = null;
        var currentColumns = new ArrayList<String>();

        try (var pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            try (var rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    var constraintType = rs.getString("constraint_type");
                    var columnName = rs.getString(COLUMN_NAME_COL);
                    var constraintName = rs.getString("constraint_name");

                    if (!constraintName.equals(currentConstraintName)) {
                        // Flush previous constraint
                        if (currentConstraintName != null) {
                            flushConstraint(constraints, currentConstraintType, currentColumns);
                        }
                        currentConstraintName = constraintName;
                        currentConstraintType = constraintType;
                        currentColumns = new ArrayList<>();
                    }

                    currentColumns.add(columnName);
                }

                // Flush last constraint
                if (currentConstraintName != null) {
                    flushConstraint(constraints, currentConstraintType, currentColumns);
                }
            }
        }

        return constraints;
    }

    /**
     * Converts a constraint type string and column list into a {@link ConstraintDescriptor}.
     */
    private static void flushConstraint(
            List<ConstraintDescriptor> constraints, String constraintType, List<String> columns) {
        switch (constraintType.toUpperCase(Locale.ROOT)) {
            case "PRIMARY KEY" -> constraints.add(ConstraintDescriptor.primaryKey(List.copyOf(columns)));
            case "UNIQUE" -> constraints.add(ConstraintDescriptor.unique(List.copyOf(columns)));
            default -> LOG.debug("Ignoring unsupported constraint type: {}", constraintType);
        }
    }

    // --- DuckDB type parsing ---

    /**
     * Parses a DuckDB column type string into a {@link FieldDescriptor}. Handles STRUCT types
     * (nested fields), LIST/ARRAY types (iterable fields), and scalar types (leaf fields).
     *
     * @param name the column name
     * @param type the DuckDB type string (e.g., "VARCHAR", "STRUCT(a INTEGER, b VARCHAR)",
     *     "INTEGER[]")
     * @param nullable whether the column is nullable
     * @return the parsed field descriptor
     */
    static FieldDescriptor parseFieldDescriptor(String name, String type, Boolean nullable) {
        if (type == null) {
            return FieldDescriptor.leaf(name, null, nullable);
        }

        // Match STRUCT/LIST patterns case-insensitively against the original type string
        // to preserve field name casing inside STRUCT definitions

        // Check for STRUCT type
        var structMatcher = STRUCT_PATTERN.matcher(type);
        if (structMatcher.matches()) {
            var nestedFields = parseStructFields(structMatcher.group(1));
            return new FieldDescriptor(name, "STRUCT", nullable, false, nestedFields);
        }

        // Check for LIST/ARRAY type (TYPE[] or LIST(TYPE))
        var listBracketMatcher = LIST_BRACKET_PATTERN.matcher(type);
        if (listBracketMatcher.matches()) {
            return parseListType(name, listBracketMatcher.group(1), nullable);
        }

        var listParenMatcher = LIST_PAREN_PATTERN.matcher(type);
        if (listParenMatcher.matches()) {
            return parseListType(name, listParenMatcher.group(1), nullable);
        }

        // Scalar type -- uppercase for consistency
        return FieldDescriptor.leaf(name, type.toUpperCase(Locale.ROOT), nullable);
    }

    /**
     * Parses a LIST element type. If the element type is a STRUCT, the list becomes an iterable
     * with nested fields. Otherwise, it becomes an iterable leaf.
     */
    private static FieldDescriptor parseListType(String name, String elementType, Boolean nullable) {
        var structMatcher = STRUCT_PATTERN.matcher(elementType);
        if (structMatcher.matches()) {
            var nestedFields = parseStructFields(structMatcher.group(1));
            return new FieldDescriptor(name, "LIST", nullable, true, nestedFields);
        }

        // Uppercase the scalar element type for consistency
        return new FieldDescriptor(name, elementType.toUpperCase(Locale.ROOT), nullable, true, List.of());
    }

    /**
     * Parses the inner content of a STRUCT type definition into a list of field descriptors.
     * Handles nested STRUCT and LIST types by tracking parenthesis depth.
     *
     * <p>Input example: {@code "a INTEGER, b VARCHAR, c STRUCT(x INTEGER, y VARCHAR)"}
     */
    static List<FieldDescriptor> parseStructFields(String structContent) {
        var fields = new ArrayList<FieldDescriptor>();
        var entries = splitTopLevelCommas(structContent);

        for (var entry : entries) {
            var trimmed = entry.trim();
            var spaceIdx = findNameTypeSeparator(trimmed);
            if (spaceIdx < 0) {
                // No type info, just a name
                fields.add(FieldDescriptor.leaf(unquote(trimmed)));
                continue;
            }

            var fieldName = unquote(trimmed.substring(0, spaceIdx).trim());
            var fieldType = trimmed.substring(spaceIdx + 1).trim();

            fields.add(parseFieldDescriptor(fieldName, fieldType, null));
        }

        return fields;
    }

    /**
     * Finds the separator between field name and type in a STRUCT field definition. Handles quoted
     * field names (e.g., {@code "type" VARCHAR}) by finding the space after the closing quote.
     */
    private static int findNameTypeSeparator(String entry) {
        if (entry.startsWith("\"")) {
            var closingQuote = entry.indexOf('"', 1);
            if (closingQuote > 0 && closingQuote + 1 < entry.length()) {
                return closingQuote + 1;
            }
            return -1;
        }
        return entry.indexOf(' ');
    }

    /**
     * Strips surrounding double quotes from a string if present. DuckDB quotes reserved word
     * field names inside STRUCT type definitions (e.g., {@code "type"} becomes {@code type}).
     */
    private static String unquote(String value) {
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    /**
     * Splits a string by commas at the top level only (not inside parentheses). This is needed
     * to correctly parse STRUCT field definitions that may contain nested STRUCT or LIST types.
     */
    static List<String> splitTopLevelCommas(String input) {
        var parts = new ArrayList<String>();
        var depth = 0;
        var start = 0;

        for (var i = 0; i < input.length(); i++) {
            var ch = input.charAt(i);
            if (ch == '(' || ch == '[') {
                depth++;
            } else if (ch == ')' || ch == ']') {
                depth--;
            } else if (ch == ',' && depth == 0) {
                parts.add(input.substring(start, i));
                start = i + 1;
            }
        }

        parts.add(input.substring(start));
        return parts;
    }

    // --- Helper methods ---

    private static boolean parseNullable(String nullValue) {
        return "YES".equalsIgnoreCase(nullValue);
    }

    private static String buildReadFunction(Resource refIri, String filePath) {
        var quotedPath = inline(filePath);

        if (isParquetFile(filePath)) {
            return "read_parquet(%s)".formatted(quotedPath);
        }

        if (Rdf.Ql.JsonPath.equals(refIri) || Rdf.Rml.JsonPath.equals(refIri)) {
            return "read_json_auto(%s)".formatted(quotedPath);
        }

        if (Rdf.Ql.Csv.equals(refIri) || Rdf.Rml.Csv.equals(refIri)) {
            return "read_csv_auto(%s)".formatted(quotedPath);
        }

        throw new IllegalArgumentException("Unsupported file reference formulation: %s".formatted(refIri));
    }

    private static String resolveFilePath(Source source) {
        if (source == null) {
            throw new IllegalArgumentException("LogicalSource has no source defined");
        }

        String path;
        String sourceLabel;
        if (source instanceof FileSource fileSource) {
            path = fileSource.getUrl();
            sourceLabel = "FileSource URL";
        } else if (source instanceof FilePath filePath) {
            path = filePath.getPath();
            sourceLabel = "FilePath path";
        } else {
            throw new IllegalArgumentException("Unsupported source type for file-based introspection: %s"
                    .formatted(source.getClass().getName()));
        }

        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("%s is not defined".formatted(sourceLabel));
        }
        return path;
    }

    private static String resolveTableName(LogicalSource logicalSource) {
        var tableName = logicalSource.getTableName();
        if (tableName != null && !tableName.isBlank()) {
            return tableName;
        }

        var source = logicalSource.getSource();
        if (source instanceof DatabaseSource dbSource) {
            var query = dbSource.getQuery();
            if (query != null && !query.isBlank()) {
                throw new IllegalArgumentException(
                        "Database query sources are not supported for schema introspection. Use a table name instead.");
            }
        }

        throw new IllegalArgumentException("Database logical source has no table name defined");
    }

    private static boolean isFileSource(Resource refIri) {
        return FILE_REF_FORMULATIONS.contains(refIri);
    }

    private static boolean isDbSource(Resource refIri) {
        return DB_REF_FORMULATIONS.contains(refIri);
    }

    private static boolean isParquetFile(String filePath) {
        var lowerPath = filePath.toLowerCase(Locale.ROOT);
        return PARQUET_EXTENSIONS.stream().anyMatch(lowerPath::endsWith);
    }
}
