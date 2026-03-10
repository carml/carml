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

        var iterator = logicalSource.getIterator();
        if (iterator != null && iterator.contains("..")) {
            LOG.debug("Iterator uses recursive descent, unsupported by DuckDB: {}", iterator);
            return false;
        }

        return true;
    }

    @Override
    public CompiledSource compileSource(LogicalSource logicalSource, Set<Field> viewFields, String cteAlias) {
        var filePath = DuckDbFileSourceUtils.resolveFilePath(logicalSource.getSource());

        if (DuckDbFileSourceUtils.isParquetFile(filePath)) {
            return columnSource("read_parquet(%s)".formatted(inline(filePath)), cteAlias);
        }

        var iterator = logicalSource.getIterator();
        if (iterator != null && !iterator.isBlank()) {
            var parsed = JsonPathAnalyzer.analyze(iterator);
            var basePath = parsed.basePath();

            var sourceSql = "(select unnest(json_extract(content, %s)) as \"%s\" from read_text(%s))"
                    .formatted(inline(basePath), JSON_ITER_COLUMN, inline(filePath));

            if (!parsed.filters().isEmpty()) {
                var filterCondition = parsed.filters().stream()
                        .map(JsonPathSourceHandler::compileFilterCondition)
                        .reduce(Condition::and)
                        .orElseThrow();
                sourceSql = "(select \"%s\" from %s where %s)"
                        .formatted(JSON_ITER_COLUMN, sourceSql, CTX.renderInlined(filterCondition));
            }

            return new CompiledSource(
                    sourceSql, JsonIteratorSourceStrategy.create(viewFields, cteAlias, JSON_ITER_COLUMN));
        }

        return columnSource("read_json_auto(%s)".formatted(inline(filePath)), cteAlias);
    }

    /**
     * Translates a {@link JsonPathAnalyzer.FilterCondition} into a jOOQ {@link Condition} for use
     * in a SQL WHERE clause. The condition references the {@code __iter} column containing the JSON
     * values.
     */
    static Condition compileFilterCondition(JsonPathAnalyzer.FilterCondition filter) {
        var iterCol = field(quotedName(JSON_ITER_COLUMN));
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
        }
        throw new UnsupportedOperationException(
                "Unknown filter condition type: %s".formatted(filter.getClass().getName()));
    }

    private static CompiledSource columnSource(String sourceSql, String cteAlias) {
        return new CompiledSource(sourceSql, new ColumnSourceStrategy(cteAlias));
    }
}
