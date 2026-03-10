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

    private static final String UNNEST_FIELD = "unnest";

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
        // Determine the parent column to extract from.
        // Root level: extract from the iterator column (plain JSON from SELECT-list unnest)
        // Nested level: extract from "parent"."unnest" (STRUCT-wrapped JSON from FROM-clause unnest)
        var parentRef =
                isRootLevel ? field(quotedName(cteAlias, iterColumn)) : field(quotedName(parentAlias, UNNEST_FIELD));

        if (iterator.endsWith("[*]")) {
            // Array iteration: json_extract returns JSON[], unnest directly.
            // Uses LATERAL subquery with parallel unnest to produce per-parent 0-based ordinals.
            // DuckDB 1.x does not support multi-argument unnest in FROM clause, but parallel
            // unnest works in SELECT clause within a LATERAL subquery.
            return table(
                            "LATERAL (SELECT unnest(json_extract({0}, {1})) AS \""
                                    + UNNEST_FIELD + "\", unnest(range(len(json_extract({0}, {1})))) AS \""
                                    + ORDINAL_FIELD + "\")",
                            parentRef,
                            inline(iterator))
                    .as(quotedName(absoluteName));
        }
        // Single value: json_extract returns JSON, wrap in list_value for unnest.
        // Ordinal is always 0 for single-value unnest.
        return table(
                        "LATERAL (SELECT unnest(list_value(json_extract({0}, {1}))) AS \"" + UNNEST_FIELD
                                + "\", unnest(list_value(0)) AS \"" + ORDINAL_FIELD + "\")",
                        parentRef,
                        inline(iterator))
                .as(quotedName(absoluteName));
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
