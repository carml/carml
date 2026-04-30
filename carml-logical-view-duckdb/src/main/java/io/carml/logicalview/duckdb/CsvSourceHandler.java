package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.inline;

import io.carml.csv.CsvDialectConfig;
import io.carml.csv.CsvNullValueHandler;
import io.carml.csv.CsvwDialectProcessor;
import io.carml.model.ExpressionField;
import io.carml.model.ExpressionMap;
import io.carml.model.Field;
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.source.csvw.CsvwTable;
import io.carml.vocab.Rdf;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

    /**
     * Sentinel value used as DuckDB's {@code nullstr} parameter when no explicit null values are
     * configured. Prevents DuckDB from treating empty quoted strings ({@code ""}) as SQL NULL.
     */
    static final String NO_NULL_SENTINEL = "__CARML_CSVW_NO_NULL__";

    private static final Set<Resource> SUPPORTED = Set.of(Rdf.Ql.Csv, Rdf.Rml.Csv);

    @SuppressWarnings("UnstableApiUsage")
    private static final org.jooq.DSLContext CTX = DSL.using(SQLDialect.DUCKDB);

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

    /**
     * Validates that CSV column references in the view match the actual CSV file column headers
     * case-sensitively. DuckDB's {@code read_csv_auto} resolves column references
     * case-insensitively, but the RML spec requires case-sensitive matching. A reference to
     * {@code "name"} must not match a column header {@code "Name"}.
     *
     * <p>Skips validation for non-file-based sources and Parquet files.
     */
    @Override
    public void validate(LogicalView view, Connection connection) {
        var viewOn = view.getViewOn();
        if (!(viewOn instanceof LogicalSource logicalSource)) {
            return;
        }

        if (!DuckDbFileSourceUtils.isFileBasedSource(logicalSource.getSource())) {
            return;
        }

        var filePath = DuckDbFileSourceUtils.resolveFilePath(logicalSource.getSource());
        if (DuckDbFileSourceUtils.isParquetFile(filePath)) {
            return;
        }

        var actualColumnNames = queryCsvColumnNames(filePath, connection);
        if (actualColumnNames.isEmpty()) {
            return;
        }

        var fieldReferences = collectFieldReferences(view.getFields());
        for (var ref : fieldReferences) {
            // Check if any actual column matches case-insensitively but not case-sensitively
            var hasExactMatch = actualColumnNames.contains(ref);
            var hasCaseInsensitiveMatch = actualColumnNames.stream().anyMatch(col -> col.equalsIgnoreCase(ref));
            if (hasCaseInsensitiveMatch && !hasExactMatch) {
                throw new IllegalArgumentException(("CSV column reference '%s' does not match any column header"
                                + " case-sensitively. Available columns: %s")
                        .formatted(ref, actualColumnNames));
            }
        }
    }

    @Override
    public CompiledSource compileSource(
            LogicalSource logicalSource,
            Set<Field> viewFields,
            String cteAlias,
            DuckDbDatabaseAttacher databaseAttacher) {
        var source = logicalSource.getSource();
        var filePath = DuckDbFileSourceUtils.resolveFilePath(source);

        if (DuckDbFileSourceUtils.isParquetFile(filePath)) {
            return columnSource("read_parquet(%s)".formatted(inline(filePath)), cteAlias, viewFields);
        }

        if (source instanceof CsvwTable csvwTable) {
            var dialect = csvwTable.getDialect();
            if (dialect != null) {
                var dialectConfig = CsvwDialectProcessor.process(dialect);
                var nullValues = CsvNullValueHandler.resolveNullValues(csvwTable);
                return compileCsvwSource(filePath, dialectConfig, nullValues, cteAlias, viewFields);
            }
        }

        // Per the RML I/O spec, CSV has no native typing — every CSV value is a string. Use
        // read_csv with all_varchar = true so DuckDB does not auto-detect column types (which
        // would surface downstream as XSD datatypes, e.g. "33"^^xsd:integer for an integer-looking
        // column). The nullstr handling mirrors the CSVW path: honor rml:null when declared, fall
        // back to a sentinel that prevents DuckDB from treating empty cells as SQL NULL.
        var nullValues = CsvNullValueHandler.resolveNullValues(source);
        return columnSource(compilePlainCsvSourceSql(filePath, nullValues), cteAlias, viewFields);
    }

    /**
     * Compiles a {@code read_csv()} call for plain (non-CSVW) CSV sources, forcing all columns to
     * VARCHAR and routing {@code rml:null} declarations through DuckDB's {@code nullstr} parameter.
     */
    private static String compilePlainCsvSourceSql(String filePath, Set<Object> nullValues) {
        var params = new ArrayList<String>();
        params.add(CTX.render(inline(filePath)));
        params.add("all_varchar = true");
        appendNullstrParam(params, nullValues);

        return "read_csv(%s)".formatted(String.join(", ", params));
    }

    /**
     * Appends a {@code nullstr} parameter to {@code params} that mirrors the supplied null-value
     * set: a single inline literal for one entry, a bracketed list for multiple, or
     * {@link #NO_NULL_SENTINEL} when the set is empty. The sentinel suppresses DuckDB's default
     * empty-string-as-NULL behavior so that empty CSV cells survive as empty-string values —
     * matching the RML I/O spec's "no default NULL token for CSV" rule.
     */
    private static void appendNullstrParam(List<String> params, Set<Object> nullValues) {
        if (nullValues != null && !nullValues.isEmpty()) {
            if (nullValues.size() == 1) {
                params.add("nullstr = %s"
                        .formatted(
                                CTX.render(inline(nullValues.iterator().next().toString()))));
            } else {
                var nullList = nullValues.stream()
                        .sorted(Comparator.comparing(Object::toString))
                        .map(v -> CTX.render(inline(v.toString())))
                        .collect(Collectors.joining(", "));
                params.add("nullstr = [%s]".formatted(nullList));
            }
        } else {
            params.add("nullstr = %s".formatted(CTX.render(inline(NO_NULL_SENTINEL))));
        }
    }

    /**
     * Compiles a {@code read_csv()} call with explicit parameters derived from the resolved dialect
     * configuration and null values.
     *
     * <p>Maps {@link CsvDialectConfig} properties to DuckDB {@code read_csv} named parameters:
     * <ul>
     *   <li>{@code delimiter} &rarr; {@code delim}
     *   <li>{@code quoteChar} &rarr; {@code quote}
     *   <li>{@code encoding} &rarr; {@code encoding}
     *   <li>{@code commentPrefix} &rarr; {@code comment}
     *   <li>{@code skipRows} &rarr; {@code skip}
     *   <li>{@code useDoubleQuoteEscaping = false} &rarr; {@code escape = '\\'}
     *   <li>{@code trim} &rarr; wraps source in a trimming subquery
     *   <li>null values &rarr; {@code nullstr}
     * </ul>
     */
    private static CompiledSource compileCsvwSource(
            String filePath, CsvDialectConfig config, Set<Object> nullValues, String cteAlias, Set<Field> viewFields) {
        var params = new ArrayList<String>();
        params.add(CTX.render(inline(filePath)));

        config.delimiter().ifPresent(d -> params.add("delim = %s".formatted(CTX.render(inline(String.valueOf(d))))));

        config.quoteChar().ifPresent(q -> params.add("quote = %s".formatted(CTX.render(inline(String.valueOf(q))))));

        config.encoding().ifPresent(e -> params.add("encoding = %s".formatted(CTX.render(inline(e)))));

        // CSVW spec default: header = true (headerRowCount = 1). DuckDB read_csv also defaults to
        // header = true. The CarmlCsvwDialect model defaults hasHeader to false / headerRowCount to
        // 0 due to Java primitive defaults, so we cannot reliably detect explicit false. Since both
        // CSVW and DuckDB defaults align, we omit the parameter — DuckDB will use header = true.

        config.commentPrefix()
                .ifPresent(c -> params.add("comment = %s".formatted(CTX.render(inline(String.valueOf(c))))));

        if (config.skipRows() > 0) {
            params.add("skip = %d".formatted(config.skipRows()));
        }

        if (!config.useDoubleQuoteEscaping()) {
            params.add("escape = %s".formatted(CTX.render(inline("\\"))));
        }

        appendNullstrParam(params, nullValues);

        // Force all columns to VARCHAR to match RML's string-based processing model
        params.add("all_varchar = true");

        var sourceSql = "read_csv(%s)".formatted(String.join(", ", params));

        if (config.trim()) {
            sourceSql = compileTrimWrapper(sourceSql);
        }

        return columnSource(sourceSql, cteAlias, viewFields);
    }

    /**
     * Wraps the source SQL in a subquery that trims all VARCHAR columns. This handles the CSVW
     * {@code trim = true} dialect option, which DuckDB's {@code read_csv} does not support natively.
     */
    private static String compileTrimWrapper(String sourceSql) {
        return "(select columns(c -> true)::VARCHAR from (select trim(columns(*)) from %s))".formatted(sourceSql);
    }

    private static CompiledSource columnSource(String sourceSql, String cteAlias, Set<Field> viewFields) {
        return new CompiledSource(
                sourceSql,
                ColumnSourceStrategy.create(viewFields, cteAlias, ColumnSourceStrategy.TypeCompanionMode.NONE));
    }

    /**
     * Queries the actual column names from a CSV file using DuckDB's {@code read_csv_auto} with
     * {@code LIMIT 0} to read only the header.
     */
    @SuppressWarnings("java:S2077") // file path is from model, not user input
    private static List<String> queryCsvColumnNames(String filePath, Connection connection) {
        var sql = "SELECT * FROM read_csv_auto('%s') LIMIT 0".formatted(filePath.replace("'", "''"));
        try (var statement = connection.createStatement();
                var resultSet = statement.executeQuery(sql)) {
            var metadata = resultSet.getMetaData();
            var columns = new ArrayList<String>(metadata.getColumnCount());
            for (var i = 1; i <= metadata.getColumnCount(); i++) {
                columns.add(metadata.getColumnLabel(i));
            }
            return columns;
        } catch (SQLException ex) {
            LOG.debug("Could not query CSV column names for file: {}", filePath, ex);
            return List.of();
        }
    }

    /**
     * Collects all reference expressions from the view's fields, including references from nested
     * fields within iterable fields and template reference expressions.
     */
    private static Set<String> collectFieldReferences(Set<Field> fields) {
        return fields.stream().flatMap(CsvSourceHandler::extractReferences).collect(Collectors.toUnmodifiableSet());
    }

    private static Stream<String> extractReferences(Field field) {
        if (field instanceof ExpressionField exprField) {
            return extractExpressionMapReferences(exprField);
        }
        if (field instanceof IterableField iterableField) {
            return iterableField.getFields().stream().flatMap(CsvSourceHandler::extractReferences);
        }
        return Stream.empty();
    }

    private static Stream<String> extractExpressionMapReferences(ExpressionMap exprMap) {
        if (exprMap.getReference() != null) {
            return Stream.of(exprMap.getReference());
        }
        if (exprMap.getTemplate() != null) {
            return exprMap.getTemplate().getReferenceExpressions().stream()
                    .map(io.carml.model.Template.ReferenceExpression::getValue);
        }
        return Stream.empty();
    }
}
