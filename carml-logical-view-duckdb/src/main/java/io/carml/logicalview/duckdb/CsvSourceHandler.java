package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.inline;

import io.carml.model.Field;
import io.carml.model.LogicalSource;
import io.carml.model.source.csvw.CsvwDialect;
import io.carml.model.source.csvw.CsvwTable;
import io.carml.vocab.Rdf;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

/**
 * Handles CSV data sources.
 *
 * <p>Uses {@code read_csv_auto} for plain CSV files, {@code read_csv} with explicit parameters for
 * CSVW sources with dialect options, and {@code read_parquet} for Parquet files. All fields are
 * accessed via {@link ColumnSourceStrategy} (direct column references).
 */
@Slf4j
final class CsvSourceHandler implements DuckDbSourceHandler {

    private static final Set<Resource> SUPPORTED = Set.of(Rdf.Ql.Csv, Rdf.Rml.Csv);

    private static final org.jooq.DSLContext CTX = DSL.using(SQLDialect.DUCKDB);

    /**
     * Sentinel value used as {@code nullstr} parameter when no {@code csvw:null} values are
     * specified. Prevents DuckDB from treating empty quoted strings ({@code ""}) as SQL NULL.
     */
    static final String NO_NULL_SENTINEL = "__CARML_CSVW_NO_NULL__";

    @Override
    public Set<Resource> supportedFormulations() {
        return SUPPORTED;
    }

    @Override
    public boolean isCompatible(LogicalSource logicalSource) {
        if (!DuckDbFileSourceUtils.isFileBasedSource(logicalSource.getSource())) {
            LOG.debug("LogicalSource has file-based reference formulation but source is not file-based");
            return false;
        }
        return true;
    }

    @Override
    public CompiledSource compileSource(LogicalSource logicalSource, Set<Field> viewFields, String cteAlias) {
        var source = logicalSource.getSource();
        var filePath = DuckDbFileSourceUtils.resolveFilePath(source);

        if (DuckDbFileSourceUtils.isParquetFile(filePath)) {
            return columnSource("read_parquet(%s)".formatted(inline(filePath)), cteAlias);
        }

        if (source instanceof CsvwTable csvwTable) {
            var dialect = csvwTable.getDialect();
            if (dialect != null) {
                return compileCsvwSource(filePath, dialect, csvwTable.getCsvwNulls(), cteAlias);
            }
        }

        return columnSource("read_csv_auto(%s)".formatted(inline(filePath)), cteAlias);
    }

    /**
     * Compiles a {@code read_csv()} call with explicit parameters derived from CSVW dialect options
     * and null values.
     *
     * <p>Maps CSVW dialect properties to DuckDB {@code read_csv} named parameters:
     * <ul>
     *   <li>{@code csvw:delimiter} &rarr; {@code delim}
     *   <li>{@code csvw:quoteChar} &rarr; {@code quote}
     *   <li>{@code csvw:encoding} &rarr; {@code encoding}
     *   <li>{@code csvw:header} / {@code csvw:headerRowCount} &rarr; {@code header}
     *   <li>{@code csvw:commentPrefix} &rarr; {@code comment}
     *   <li>{@code csvw:skipRows} &rarr; {@code skip}
     *   <li>{@code csvw:doubleQuote} &rarr; {@code escape} (backslash when false)
     *   <li>{@code csvw:trim} &rarr; wraps source in a trimming subquery
     *   <li>{@code csvw:null} &rarr; {@code nullstr}
     * </ul>
     */
    private static CompiledSource compileCsvwSource(
            String filePath, CsvwDialect dialect, Set<Object> nullValues, String cteAlias) {
        var params = new ArrayList<String>();
        params.add(CTX.render(inline(filePath)));

        if (dialect.getDelimiter() != null) {
            params.add("delim = %s".formatted(CTX.render(inline(dialect.getDelimiter()))));
        }

        if (dialect.getQuoteChar() != null) {
            params.add("quote = %s".formatted(CTX.render(inline(dialect.getQuoteChar()))));
        }

        if (dialect.getEncoding() != null) {
            params.add("encoding = %s".formatted(CTX.render(inline(dialect.getEncoding()))));
        }

        // CSVW spec default: header = true (headerRowCount = 1). DuckDB read_csv also defaults to
        // header = true. The CarmlCsvwDialect model defaults hasHeader to false / headerRowCount to
        // 0 due to Java primitive defaults, so we cannot reliably detect explicit false. Since both
        // CSVW and DuckDB defaults align, we omit the parameter — DuckDB will use header = true.

        if (dialect.getCommentPrefix() != null) {
            params.add("comment = %s".formatted(CTX.render(inline(dialect.getCommentPrefix()))));
        }

        if (dialect.getSkipRows() > 0) {
            params.add("skip = %d".formatted(dialect.getSkipRows()));
        }

        if (dialect.getDoubleQuote() != null && "false".equalsIgnoreCase(dialect.getDoubleQuote())) {
            params.add("escape = %s".formatted(CTX.render(inline("\\"))));
        }

        if (nullValues != null && !nullValues.isEmpty()) {
            if (nullValues.size() == 1) {
                params.add("nullstr = %s"
                        .formatted(
                                CTX.render(inline(nullValues.iterator().next().toString()))));
            } else {
                var nullList = nullValues.stream()
                        .sorted(java.util.Comparator.comparing(Object::toString))
                        .map(v -> CTX.render(inline(v.toString())))
                        .collect(Collectors.joining(", "));
                params.add("nullstr = [%s]".formatted(nullList));
            }
        } else {
            // When no csvw:null values are specified, set a sentinel nullstr to prevent DuckDB
            // from treating empty quoted strings as NULL. This preserves empty strings as
            // empty string values, matching RML's expectation that only explicit null values
            // (from csvw:null) should produce NULL.
            params.add("nullstr = %s".formatted(CTX.render(inline(NO_NULL_SENTINEL))));
        }

        // Force all columns to VARCHAR to match RML's string-based processing model
        params.add("all_varchar = true");

        var sourceSql = "read_csv(%s)".formatted(String.join(", ", params));

        if (dialect.trim()) {
            sourceSql = compileTrimWrapper(sourceSql);
        }

        return columnSource(sourceSql, cteAlias);
    }

    /**
     * Wraps the source SQL in a subquery that trims all VARCHAR columns. This handles the CSVW
     * {@code trim = true} dialect option, which DuckDB's {@code read_csv} does not support natively.
     */
    private static String compileTrimWrapper(String sourceSql) {
        return "(select columns(c -> true)::VARCHAR from (select trim(columns(*)) from %s))".formatted(sourceSql);
    }

    private static CompiledSource columnSource(String sourceSql, String cteAlias) {
        return new CompiledSource(
                sourceSql, new ColumnSourceStrategy(cteAlias, ColumnSourceStrategy.TypeCompanionMode.NONE));
    }
}
