package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.table;

import io.carml.model.ExpressionField;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Source strategy for JSON sources with iterators.
 *
 * <p>Field values are extracted from a designated JSON column using {@code json_extract_string}.
 * UNNEST tables use {@code json_extract} to navigate JSON arrays and objects.
 *
 * <p>The column name for the JSON iterator rows is provided at construction time by the compiler,
 * which creates the CTE that produces this column.
 */
final class JsonIteratorSourceStrategy implements DuckDbSourceStrategy {

    private static final String JSON_EXTRACT_STRING = "json_extract_string({0}, {1})";

    static final String UNNEST_FIELD = "unnest";

    private final String cteAlias;

    private final String iterColumn;

    private final Map<String, String> fieldNameToRefMap;

    private JsonIteratorSourceStrategy(String cteAlias, String iterColumn, Map<String, String> fieldNameToRefMap) {
        this.cteAlias = cteAlias;
        this.iterColumn = iterColumn;
        this.fieldNameToRefMap = fieldNameToRefMap;
    }

    static JsonIteratorSourceStrategy create(Set<io.carml.model.Field> viewFields, String cteAlias, String iterColumn) {
        var fieldNameToRefMap = viewFields.stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .filter(f -> f.getReference() != null)
                .collect(Collectors.toUnmodifiableMap(ExpressionField::getFieldName, ExpressionField::getReference));
        return new JsonIteratorSourceStrategy(cteAlias, iterColumn, fieldNameToRefMap);
    }

    @Override
    public boolean isMultiValuedReference(String reference) {
        if (reference == null) {
            return false;
        }
        var parsed = JsonPathAnalyzer.analyze(reference);
        return parsed.basePath().contains("[*]") || parsed.basePath().contains(".*") || parsed.hasDeepScan();
    }

    @Override
    public SelectField<?> compileFieldReference(String reference, Name fieldAlias) {
        return DSL.field(JSON_EXTRACT_STRING, field(quotedName(cteAlias, iterColumn)), inline(reference))
                .as(fieldAlias);
    }

    @Override
    public Field<?> compileTemplateReference(String segmentValue) {
        return DSL.field(JSON_EXTRACT_STRING, field(quotedName(cteAlias, iterColumn)), inline(segmentValue));
    }

    @Override
    public SelectField<?> compileNestedFieldReference(String unnestAlias, String reference, Name fieldAlias) {
        // DuckDB's FROM-clause unnest wraps results in STRUCT(unnest JSON),
        // so access the inner JSON value via the "unnest" field.
        return DSL.field(JSON_EXTRACT_STRING, field(quotedName(unnestAlias, UNNEST_FIELD)), inline(reference))
                .as(fieldAlias);
    }

    @Override
    public Table<?> compileUnnestTable(String iterator, String parentAlias, boolean isRootLevel, String absoluteName) {
        var parsed = JsonPathAnalyzer.analyze(iterator);
        var basePath = parsed.basePath();

        validateUnsupportedFeatures(iterator, parsed);

        // Use the raw iterator for the SQL expression so DuckDB applies filter expressions natively.
        // Only fall back to the normalized basePath when there are no filters.
        var sqlPath = parsed.filters().isEmpty() ? basePath : iterator;

        // Root level: extract from the iterator column (plain JSON from SELECT-list unnest)
        // Nested level: extract from "parent"."unnest" (STRUCT-wrapped JSON from FROM-clause unnest)
        var parentRef =
                isRootLevel ? field(quotedName(cteAlias, iterColumn)) : field(quotedName(parentAlias, UNNEST_FIELD));

        if (basePath.endsWith("[*]")) {
            return compileArrayUnnest(parsed, sqlPath, parentRef, absoluteName);
        }

        // Single value: json_extract returns JSON, wrap in list_value for unnest.
        return table("""
                        LATERAL (SELECT unnest(list_value(json_extract({0}, {1}))) AS "%s", \
                        unnest(list_value(0)) AS "%s")\
                        """.formatted(UNNEST_FIELD, ORDINAL_FIELD), parentRef, inline(sqlPath))
                .as(quotedName(absoluteName));
    }

    private static void validateUnsupportedFeatures(String iterator, JsonPathAnalyzer.ParsedJsonPath parsed) {
        if (parsed.hasDeepScan()) {
            throw new UnsupportedOperationException(
                    "Recursive descent (..) in '%s' is not supported in DuckDB UNNEST".formatted(iterator));
        }
        if (parsed.basePath().contains(".*")) {
            throw new UnsupportedOperationException(
                    "Child wildcard (.*) in '%s' is not supported in DuckDB UNNEST".formatted(iterator));
        }
    }

    private Table<?> compileArrayUnnest(
            JsonPathAnalyzer.ParsedJsonPath parsed, String sqlPath, Field<?> parentRef, String absoluteName) {
        // Check for slice selector with actual bounds ([:] falls through to normal unnest)
        var sliceTable = compileSliceUnnest(parsed, sqlPath, parentRef, absoluteName);
        if (sliceTable != null) {
            return sliceTable;
        }

        // Standard array unnest with parallel ordinal generation
        return table("""
                        LATERAL (SELECT unnest(json_extract({0}, {1})) AS "%s", \
                        unnest(range(len(json_extract({0}, {1})))) AS "%s")\
                        """.formatted(UNNEST_FIELD, ORDINAL_FIELD), parentRef, inline(sqlPath))
                .as(quotedName(absoluteName));
    }

    private static Table<?> compileSliceUnnest(
            JsonPathAnalyzer.ParsedJsonPath parsed, String sqlPath, Field<?> parentRef, String absoluteName) {
        if (parsed.slices().isEmpty()) {
            return null;
        }

        // Slice selector: unnest full array, filter by ordinal range, recompute ordinals.
        // JSONPath slices are 0-based start-inclusive, end-exclusive.
        // [:]  (both null) is equivalent to [*], so return null to fall through.
        var slice = parsed.slices().get(parsed.slices().size() - 1);
        if (slice.start() == null && slice.end() == null) {
            return null;
        }

        var whereClause = buildSliceWhereClause(slice);

        return table("""
                        LATERAL (SELECT "%1$s", (row_number() over() - 1) AS "%2$s" \
                        FROM (SELECT unnest(json_extract({0}, {1})) AS "%1$s", \
                        unnest(range(len(json_extract({0}, {1})))) AS "%2$s") \
                        WHERE %3$s)\
                        """.formatted(UNNEST_FIELD, ORDINAL_FIELD, whereClause), parentRef, inline(sqlPath))
                .as(quotedName(absoluteName));
    }

    private static String buildSliceWhereClause(JsonPathAnalyzer.SliceSelector slice) {
        var startCondition = slice.start() != null ? "\"" + ORDINAL_FIELD + "\" >= " + slice.start() : null;
        var endCondition = slice.end() != null ? "\"" + ORDINAL_FIELD + "\" < " + slice.end() : null;

        if (startCondition != null && endCondition != null) {
            return startCondition + " AND " + endCondition;
        }
        return startCondition != null ? startCondition : endCondition;
    }

    @Override
    public SelectField<?> compileFieldTypeReference(String reference, Name typeAlias) {
        return DSL.field("json_type({0}, {1})", field(quotedName(cteAlias, iterColumn)), inline(reference))
                .as(typeAlias);
    }

    @Override
    public SelectField<?> compileNestedFieldTypeReference(String unnestAlias, String reference, Name typeAlias) {
        return DSL.field("json_type({0}, {1})", field(quotedName(unnestAlias, UNNEST_FIELD)), inline(reference))
                .as(typeAlias);
    }

    @Override
    public Field<Object> resolveJoinChildReference(String childRef) {
        var sourceRef = fieldNameToRefMap.get(childRef);
        if (sourceRef != null) {
            return DSL.field(JSON_EXTRACT_STRING, field(quotedName(cteAlias, iterColumn)), inline(sourceRef));
        }
        return field(quotedName(cteAlias, childRef));
    }
}
