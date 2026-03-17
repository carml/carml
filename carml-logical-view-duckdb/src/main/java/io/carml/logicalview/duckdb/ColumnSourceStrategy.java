package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.castNull;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.table;

import io.carml.model.ExpressionField;
import java.util.Optional;
import java.util.Set;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Source strategy for column-based sources where field references map directly to table columns.
 *
 * <p>All field references are compiled as direct column references qualified by the CTE alias.
 *
 * @param cteAlias the alias of the CTE containing the source data
 * @param typeCompanionMode controls how type companion columns are compiled:
 *     <ul>
 *       <li>{@link TypeCompanionMode#NONE} — CSV and {@code read_json_auto}: no type info, compiles
 *           as {@code CAST(NULL AS VARCHAR)}
 *       <li>{@link TypeCompanionMode#SQL_TYPEOF} — SQL sources: compiles as {@code typeof("cte"."column")}
 *           to capture the static column type
 *       <li>{@link TypeCompanionMode#INNER_VIEW} — view-on-view: projects the inner view's
 *           {@code .__type} companion column
 *     </ul>
 */
record ColumnSourceStrategy(String cteAlias, TypeCompanionMode typeCompanionMode) implements DuckDbSourceStrategy {

    /**
     * Controls how type companion columns are compiled for column-based sources.
     */
    enum TypeCompanionMode {
        /** No type information available (CSV, {@code read_json_auto}). */
        NONE,
        /** SQL sources: use DuckDB {@code typeof()} to capture static column types. */
        SQL_TYPEOF,
        /** View-on-view: project the inner view's existing {@code .__type} companion column. */
        INNER_VIEW
    }

    @Override
    public boolean isMultiValuedReference(String reference) {
        return false;
    }

    @Override
    public SelectField<?> compileFieldReference(String reference, Name fieldAlias) {
        // Use template syntax to prevent jOOQ from stripping the table qualifier when
        // the alias matches the column name. Without this, "view_source"."id" AS "id"
        // can be simplified to just "id", causing ambiguous column errors when joins
        // introduce identically-named columns from the parent table.
        return DSL.field("{0}", field(quotedName(cteAlias, reference))).as(fieldAlias);
    }

    @Override
    public Field<?> compileTemplateReference(String segmentValue) {
        return field(quotedName(cteAlias, segmentValue));
    }

    @Override
    public SelectField<?> compileNestedFieldReference(String unnestAlias, String reference, Name fieldAlias) {
        return field(quotedName(unnestAlias, reference)).as(fieldAlias);
    }

    @Override
    public Table<?> compileUnnestTable(String iterator, String parentAlias, boolean isRootLevel, String absoluteName) {
        // Uses LATERAL subquery with parallel unnest to produce per-parent 0-based ordinals.
        // DuckDB 1.x does not support multi-argument unnest in FROM clause, but parallel
        // unnest works in SELECT clause within a LATERAL subquery.
        return table(
                        "LATERAL (SELECT unnest({0}) AS \"unnest\", unnest(range(len({0}))) AS \"" + ORDINAL_FIELD
                                + "\")",
                        field(quotedName(parentAlias, iterator)))
                .as(quotedName(absoluteName));
    }

    @Override
    public SelectField<?> compileFieldTypeReference(String reference, Name typeAlias) {
        return compileTypeReference(cteAlias, reference, typeAlias);
    }

    @Override
    public SelectField<?> compileNestedFieldTypeReference(String unnestAlias, String reference, Name typeAlias) {
        return compileTypeReference(unnestAlias, reference, typeAlias);
    }

    private SelectField<?> compileTypeReference(String tableAlias, String reference, Name typeAlias) {
        return switch (typeCompanionMode) {
            case INNER_VIEW ->
                field(quotedName(tableAlias, reference + TYPE_SUFFIX)).as(typeAlias);
            case SQL_TYPEOF ->
                DSL.field("typeof({0})", field(quotedName(tableAlias, reference))).as(typeAlias);
            case NONE -> castNull(String.class).as(typeAlias);
        };
    }

    @Override
    public Field<Object> resolveJoinChildReference(String childRef) {
        return field(quotedName(cteAlias, childRef));
    }

    @Override
    public Set<String> nonScalarTypeValues() {
        return Set.of();
    }

    @Override
    public Optional<String> sourceEvaluationColumn() {
        return Optional.empty();
    }

    @Override
    public UnnestDescriptor compileMultiValuedUnnestDescriptor(ExpressionField field, String cteAlias) {
        throw new UnsupportedOperationException("Column-based sources do not support multi-valued expression fields");
    }
}
