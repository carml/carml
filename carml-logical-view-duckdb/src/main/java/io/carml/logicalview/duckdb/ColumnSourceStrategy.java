package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.castNull;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.table;

import io.carml.model.ExpressionField;
import io.carml.model.ExpressionMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
 * <p>For join-condition resolution, references in the join's {@link io.carml.model.ChildMap} may
 * be either underlying-source column names or view field names (when the view is an explicit
 * {@code rml:LogicalView} where {@code fieldName} differs from {@code reference}). The strategy
 * uses {@link #fieldNameToRefMap} to translate field names back to the underlying column name
 * before emitting the SQL reference.
 */
final class ColumnSourceStrategy implements DuckDbSourceStrategy {

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

    private final String cteAlias;

    private final TypeCompanionMode typeCompanionMode;

    private final Map<String, String> fieldNameToRefMap;

    ColumnSourceStrategy(String cteAlias, TypeCompanionMode typeCompanionMode) {
        this(cteAlias, typeCompanionMode, Map.of());
    }

    private ColumnSourceStrategy(
            String cteAlias, TypeCompanionMode typeCompanionMode, Map<String, String> fieldNameToRefMap) {
        this.cteAlias = cteAlias;
        this.typeCompanionMode = typeCompanionMode;
        this.fieldNameToRefMap = fieldNameToRefMap;
    }

    static ColumnSourceStrategy create(
            Set<io.carml.model.Field> viewFields, String cteAlias, TypeCompanionMode typeCompanionMode) {
        var fieldNameToRefMap = viewFields.stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .filter(f -> f.getReference() != null)
                .collect(Collectors.toUnmodifiableMap(ExpressionField::getFieldName, ExpressionField::getReference));
        return new ColumnSourceStrategy(cteAlias, typeCompanionMode, fieldNameToRefMap);
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
                DSL.field("typeof({0})", field(quotedName(tableAlias, reference)))
                        .as(typeAlias);
            case NONE -> castNull(String.class).as(typeAlias);
        };
    }

    @Override
    public Field<?> resolveJoinChildExpression(ExpressionMap childMap) {
        return DuckDbViewCompiler.compileJoinExpressionMap(
                childMap,
                reference -> field(quotedName(cteAlias, resolveColumnRef(reference))),
                templateVariable -> field(quotedName(cteAlias, resolveColumnRef(templateVariable))));
    }

    /**
     * Resolves a join-condition reference or template variable to the underlying column name. When
     * the reference is a view field name (the view is an explicit {@code rml:LogicalView} where
     * {@code fieldName} differs from {@code reference}), it is rewritten to the underlying source
     * reference. Otherwise the reference is returned as-is so that direct underlying-column names
     * still resolve.
     */
    private String resolveColumnRef(String reference) {
        var sourceRef = fieldNameToRefMap.get(reference);
        return sourceRef != null ? sourceRef : reference;
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
