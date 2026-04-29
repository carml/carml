package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.quotedName;

import io.carml.model.Field;
import io.carml.model.LogicalSource;
import io.carml.vocab.Rdf;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import org.jooq.Condition;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

/**
 * Handles JSON data sources with JsonPath reference formulations.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li><b>Iterator mode</b>: uses {@code read_text} + {@code json_extract} + {@code UNNEST} to
 *       expand an iterator path into rows, with {@link JsonIteratorSourceStrategy} for field
 *       extraction via {@code json_extract_string}.</li>
 *   <li><b>Auto mode</b>: uses {@code read_json_auto} for sources without an iterator, with
 *       {@link ColumnSourceStrategy} for direct column access.</li>
 * </ul>
 *
 * <p>Parquet files are detected by extension and read via {@code read_parquet} regardless of mode.
 *
 * <p>JSONPath filter expressions in iterators are translated to SQL WHERE clauses via
 * {@link JsonPathAnalyzer}.
 */
@Slf4j
final class JsonPathSourceHandler implements DuckDbSourceHandler {

    static final String JSON_ITER_COLUMN = "__iter";

    private static final Set<Resource> SUPPORTED = Set.of(Rdf.Ql.JsonPath, Rdf.Rml.JsonPath);

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

    @Override
    public CompiledSource compileSource(
            LogicalSource logicalSource,
            Set<Field> viewFields,
            String cteAlias,
            DuckDbDatabaseAttacher databaseAttacher) {
        var filePath = DuckDbFileSourceUtils.resolveFilePath(logicalSource.getSource());

        if (DuckDbFileSourceUtils.isParquetFile(filePath)) {
            return columnSource("read_parquet(%s)".formatted(inline(filePath)), cteAlias, viewFields);
        }

        var iterator = logicalSource.getIterator();
        if (iterator != null && !iterator.isBlank()) {
            var parsed = JsonPathAnalyzer.analyze(iterator);
            // DuckDB's json_extract treats ".*" as object-property-only wildcard and does not
            // match array elements. JSONPath semantics treat ".*" and "[*]" equivalently (both
            // match all children). Normalize ".*" → "[*]" so DuckDB iterates arrays correctly.
            var basePath = normalizeChildWildcard(parsed.basePath());

            var sourceSql = "(select unnest(json_extract(content, %s)) as \"%s\" from read_text(%s))"
                    .formatted(inline(basePath), JSON_ITER_COLUMN, inline(filePath));

            if (!parsed.filters().isEmpty()) {
                var filterCondition = parsed.filters().stream()
                        .map(f -> compileFilterCondition(f, JSON_ITER_COLUMN))
                        .reduce(Condition::and)
                        .orElseThrow();
                sourceSql = "(select \"%s\" from %s where %s)"
                        .formatted(JSON_ITER_COLUMN, sourceSql, CTX.renderInlined(filterCondition));
            }

            return new CompiledSource(
                    sourceSql, JsonIteratorSourceStrategy.create(viewFields, cteAlias, JSON_ITER_COLUMN));
        }

        return columnSource("read_json_auto(%s)".formatted(inline(filePath)), cteAlias, viewFields);
    }

    /**
     * Translates a {@link JsonPathAnalyzer.FilterCondition} into a jOOQ {@link Condition} for use
     * in a SQL WHERE clause. The condition references the specified column containing the JSON
     * values.
     *
     * @param filter the filter condition to translate
     * @param columnName the column to apply the filter to (e.g., {@code __iter} or {@code unnest})
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    static Condition compileFilterCondition(JsonPathAnalyzer.FilterCondition filter, String columnName) {
        var iterCol = field(quotedName(columnName));
        if (filter instanceof JsonPathAnalyzer.EqualStr f) {
            return DSL.condition(
                    "json_extract_string({0}, {1}) = {2}", iterCol, inline(f.fieldJsonPath()), inline(f.value()));
        } else if (filter instanceof JsonPathAnalyzer.EqualNum f) {
            return DSL.condition(
                    "cast(json_extract_string({0}, {1}) as double) = {2}",
                    iterCol, inline(f.fieldJsonPath()), inline(f.value().doubleValue()));
        } else if (filter instanceof JsonPathAnalyzer.EqualBool f) {
            return DSL.condition(
                    "cast(json_extract_string({0}, {1}) as boolean) = {2}",
                    iterCol, inline(f.fieldJsonPath()), inline(f.value()));
        } else if (filter instanceof JsonPathAnalyzer.GreaterThanNum f) {
            return DSL.condition(
                    "cast(json_extract_string({0}, {1}) as double) > {2}",
                    iterCol, inline(f.fieldJsonPath()), inline(f.value().doubleValue()));
        } else if (filter instanceof JsonPathAnalyzer.LessThanNum f) {
            return DSL.condition(
                    "cast(json_extract_string({0}, {1}) as double) < {2}",
                    iterCol, inline(f.fieldJsonPath()), inline(f.value().doubleValue()));
        } else if (filter instanceof JsonPathAnalyzer.Exists f) {
            return DSL.condition("json_extract({0}, {1}) is not null", iterCol, inline(f.fieldJsonPath()));
        } else if (filter instanceof JsonPathAnalyzer.MatchRegex f) {
            return DSL.condition(
                    "regexp_matches(json_extract_string({0}, {1}), {2})",
                    iterCol, inline(f.fieldJsonPath()), inline(f.pattern()));
        } else if (filter instanceof JsonPathAnalyzer.LengthCompare f) {
            return compileLengthCompare(f, iterCol);
        } else if (filter instanceof JsonPathAnalyzer.FullMatch f) {
            return DSL.condition(
                    "regexp_full_match(json_extract_string({0}, {1}), {2})",
                    iterCol, inline(f.fieldJsonPath()), inline(f.pattern()));
        } else if (filter instanceof JsonPathAnalyzer.PartialMatch f) {
            return DSL.condition(
                    "regexp_matches(json_extract_string({0}, {1}), {2})",
                    iterCol, inline(f.fieldJsonPath()), inline(f.pattern()));
        } else if (filter instanceof JsonPathAnalyzer.AndFilter f) {
            return compileFilterCondition(f.left(), columnName).and(compileFilterCondition(f.right(), columnName));
        } else if (filter instanceof JsonPathAnalyzer.OrFilter f) {
            return compileFilterCondition(f.left(), columnName).or(compileFilterCondition(f.right(), columnName));
        } else if (filter instanceof JsonPathAnalyzer.NotFilter f) {
            return compileFilterCondition(f.condition(), columnName).not();
        }
        throw new UnsupportedOperationException(
                "Unknown filter condition type: %s".formatted(filter.getClass().getName()));
    }

    /**
     * Compiles a {@link JsonPathAnalyzer.LengthCompare} condition into a SQL expression that
     * measures the length of a JSON value and compares it. The length is type-aware: arrays use
     * {@code json_array_length}, while strings and objects use {@code length()}.
     */
    private static Condition compileLengthCompare(
            JsonPathAnalyzer.LengthCompare field, org.jooq.Field<Object> iterCol) {
        var pathInline = inline(field.fieldJsonPath());
        var lengthExpr = DSL.field(
                "CASE json_type({0}, {1}) WHEN {2} THEN json_array_length(json_extract({0}, {1}))"
                        + " ELSE length(json_extract_string({0}, {1})) END",
                iterCol, pathInline, inline("ARRAY"));
        var val = inline(field.value().doubleValue());

        if (field.op() == JsonPathAnalyzer.CompOp.EQ) {
            return DSL.condition("{0} = {1}", lengthExpr, val);
        } else if (field.op() == JsonPathAnalyzer.CompOp.NEQ) {
            return DSL.condition("{0} != {1}", lengthExpr, val);
        } else if (field.op() == JsonPathAnalyzer.CompOp.GT) {
            return DSL.condition("{0} > {1}", lengthExpr, val);
        } else if (field.op() == JsonPathAnalyzer.CompOp.LT) {
            return DSL.condition("{0} < {1}", lengthExpr, val);
        } else if (field.op() == JsonPathAnalyzer.CompOp.GTE) {
            return DSL.condition("{0} >= {1}", lengthExpr, val);
        } else if (field.op() == JsonPathAnalyzer.CompOp.LTE) {
            return DSL.condition("{0} <= {1}", lengthExpr, val);
        }
        throw new UnsupportedOperationException("Unknown CompOp: %s".formatted(field.op()));
    }

    /**
     * Normalizes JSONPath child wildcard notation for DuckDB compatibility. DuckDB's
     * {@code json_extract} does not treat {@code .*} as an array element wildcard; it only matches
     * object properties. JSONPath semantics define {@code .*} and {@code [*]} as equivalent (both
     * select all children). This method replaces {@code .*} with {@code [*]} so that DuckDB
     * correctly iterates both array elements and object properties.
     *
     * @param path the JSONPath base path from the analyzer
     * @return the path with {@code .*} replaced by {@code [*]}
     */
    static String normalizeChildWildcard(String path) {
        return path.replace(".*", "[*]");
    }

    private static CompiledSource columnSource(String sourceSql, String cteAlias, Set<Field> viewFields) {
        return new CompiledSource(
                sourceSql,
                ColumnSourceStrategy.create(viewFields, cteAlias, ColumnSourceStrategy.TypeCompanionMode.NONE));
    }
}
