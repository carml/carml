package io.carml.logicalview.sql;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.rowNumber;
import static org.jooq.impl.DSL.sql;
import static org.jooq.impl.DSL.table;

import io.carml.logicalview.DedupStrategy;
import io.carml.logicalview.EvaluationContext;
import io.carml.model.DatabaseSource;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.IterableField;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.StructuralAnnotation;
import io.carml.model.Template;
import io.carml.model.UniqueAnnotation;
import io.carml.model.impl.CarmlTemplate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.SelectFromStep;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.conf.ParseUnknownFunctions;
import org.jooq.conf.Settings;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

/**
 * Compiles a {@link LogicalView} into a SQL query targeting a source database dialect (MySQL,
 * PostgreSQL, etc.) using jOOQ's type-safe DSL.
 *
 * <p>This compiler handles SQL database sources only — field references map directly to table
 * columns. Unlike the DuckDB view compiler, there is no JSON extraction, UNNEST, or
 * multi-formulation handling. SQL sources are inherently flat/columnar.
 *
 * <p>Produces CTE-structured SQL with:
 * <ul>
 *   <li>{@code WITH view_source AS (SELECT *, ROW_NUMBER() OVER() AS __idx FROM ...)} for stable
 *       row indexing</li>
 *   <li>Field projection as direct column references</li>
 *   <li>Template fields as SQL CONCAT</li>
 *   <li>LEFT JOIN / INNER JOIN for logical view joins with recursive parent view compilation</li>
 *   <li>DISTINCT for deduplication (with annotation-based optimization to skip when unnecessary)</li>
 *   <li>LIMIT for capping result size</li>
 * </ul>
 *
 * <p>Structural annotation optimizations:
 * <ul>
 *   <li>PrimaryKey or Unique+NotNull covering selected fields: omits DISTINCT</li>
 *   <li>ForeignKey with no projected parent fields: eliminates JOIN entirely</li>
 *   <li>NotNull on child join keys: upgrades LEFT JOIN to INNER JOIN</li>
 * </ul>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ReactiveSqlViewCompiler {

    static final String INDEX_COLUMN = "__idx";

    private static final String CTE_ALIAS = "view_source";

    private static final String DEDUPED_ALIAS = "deduped";

    private static final String PARENT_ALIAS_PREFIX = "parent_";

    private static final String SUBQUERY_TEMPLATE = "({0})";

    /**
     * Tracks views currently being compiled on the current thread. Used to detect cycles in
     * view-on-view composition.
     */
    private static final ThreadLocal<Set<LogicalView>> COMPILING_VIEWS = ThreadLocal.withInitial(HashSet::new);

    /**
     * Holds the SQL dialect for the current compilation thread. Set by the outermost compile call
     * and inherited by recursive calls for parent views.
     */
    private static final ThreadLocal<SQLDialect> CURRENT_DIALECT = new ThreadLocal<>();

    /**
     * Cached class reference for the none dedup strategy, used to detect whether deduplication
     * should be applied.
     */
    private static final Class<? extends DedupStrategy> NONE_DEDUP_CLASS =
            DedupStrategy.none().getClass();

    /**
     * Describes a SQL JOIN derived from a {@link LogicalViewJoin}.
     */
    private record JoinDescriptor(
            Table<?> table, Condition condition, List<SelectField<?>> fields, boolean isLeftJoin) {}

    /**
     * Compiles a {@link LogicalView} into a SQL query string for the given database dialect.
     *
     * @param view the logical view defining fields and the underlying data source
     * @param context the evaluation context controlling projection, dedup, and limits
     * @param dialect the jOOQ SQL dialect for the target database
     * @return the compiled SQL query string
     * @throws IllegalArgumentException if the view contains unsupported source types or cycles
     */
    public static String compile(LogicalView view, EvaluationContext context, SQLDialect dialect) {
        var isOutermostDialectCall = CURRENT_DIALECT.get() == null;
        if (isOutermostDialectCall) {
            CURRENT_DIALECT.set(dialect);
        }
        try {
            return doCompileWithCycleDetection(view, context);
        } finally {
            if (isOutermostDialectCall) {
                CURRENT_DIALECT.remove();
            }
        }
    }

    private static String doCompileWithCycleDetection(LogicalView view, EvaluationContext context) {
        var compilingViews = COMPILING_VIEWS.get();
        var isOutermostCall = compilingViews.isEmpty();
        if (!compilingViews.add(view)) {
            throw new IllegalArgumentException(
                    "Cycle detected in view-on-view composition: view [%s] is already being compiled"
                            .formatted(view.getResourceName()));
        }

        try {
            return doCompile(view, context);
        } finally {
            compilingViews.remove(view);
            if (isOutermostCall) {
                COMPILING_VIEWS.remove();
            }
        }
    }

    private static String doCompile(LogicalView view, EvaluationContext context) {
        var dialect = CURRENT_DIALECT.get();
        var ctx = DSL.using(dialect);
        var viewOn = view.getViewOn();

        String sourceTable;
        if (viewOn instanceof LogicalView innerView) {
            var innerSql = doCompileWithCycleDetection(innerView, EvaluationContext.defaults());
            sourceTable = "(%s)".formatted(innerSql);
        } else if (viewOn instanceof LogicalSource logicalSource) {
            sourceTable = compileSourceClause(logicalSource);
        } else {
            throw new IllegalArgumentException("Unsupported viewOn target type: %s"
                    .formatted(viewOn.getClass().getName()));
        }

        var expressionFields = resolveExpressionFields(view.getFields(), context.getProjectedFields());

        var annotations = view.getStructuralAnnotations();
        var notNullFieldNames = extractNotNullFieldNames(annotations);

        var joinDescriptors = compileJoins(
                view, ctx, annotations, notNullFieldNames, context.getProjectedFields(), context.getAggregatingJoins());

        var dedupRequested = !NONE_DEDUP_CLASS.isInstance(context.getDedupStrategy());
        var useDistinct = dedupRequested && !canSkipDistinct(annotations, notNullFieldNames, view, context);

        var viewSourceCte = name(CTE_ALIAS)
                .as(ctx.select(asterisk(), rowNumber().over().as(name(INDEX_COLUMN)))
                        .from(sql(sourceTable)));

        var allFieldSelects = collectAllFieldSelects(expressionFields, joinDescriptors);
        String compiledSql;

        if (useDistinct) {
            var dedupFrom =
                    buildFromClause(ctx.selectDistinct(allFieldSelects.toArray(SelectField[]::new)), joinDescriptors);

            var dedupedCte = name(DEDUPED_ALIAS).as(dedupFrom);

            var outerQuery = ctx.with(viewSourceCte)
                    .with(dedupedCte)
                    .select(asterisk(), rowNumber().over().as(name(INDEX_COLUMN)))
                    .from(table(name(DEDUPED_ALIAS)));

            compiledSql = context.getLimit()
                    .map(limit -> outerQuery.limit(limit).getSQL())
                    .orElseGet(outerQuery::getSQL);
        } else {
            allFieldSelects.add(field(quotedName(CTE_ALIAS, INDEX_COLUMN)));

            var fromStep = buildFromClause(ctx.with(viewSourceCte).select(allFieldSelects), joinDescriptors);

            compiledSql = context.getLimit()
                    .map(limit -> fromStep.limit(limit).getSQL())
                    .orElseGet(fromStep::getSQL);
        }

        LOG.debug(
                "Compiled reactive SQL for view [{}] (dialect: {}):\n{}", view.getResourceName(), dialect, compiledSql);
        return compiledSql;
    }

    // --- Source compilation ---

    private static String compileSourceClause(LogicalSource logicalSource) {
        var query = logicalSource.getQuery();
        if (query != null && !query.isBlank()) {
            return "(%s) __src".formatted(validateAndRenderQuery(query));
        }

        var tableName = logicalSource.getTableName();
        if (tableName != null && !tableName.isBlank()) {
            return quotedName(tableName).toString();
        }

        var source = logicalSource.getSource();
        if (source instanceof DatabaseSource dbSource
                && dbSource.getQuery() != null
                && !dbSource.getQuery().isBlank()) {
            return "(%s) __src".formatted(validateAndRenderQuery(dbSource.getQuery()));
        }

        throw new IllegalArgumentException("SQL logical source has no query or table name defined");
    }

    /**
     * Validates a user-provided SQL query by parsing it through jOOQ's SQL parser and re-rendering
     * it in the target dialect. This ensures:
     * <ul>
     *   <li>The query is syntactically valid SQL (rejects malformed input)</li>
     *   <li>Only a single SELECT statement is allowed (rejects stacked queries like
     *       {@code SELECT 1; DROP TABLE users})</li>
     *   <li>Identifiers are consistently quoted in the target dialect</li>
     * </ul>
     *
     * @throws IllegalArgumentException if the query cannot be parsed as a valid SELECT statement
     */
    private static String validateAndRenderQuery(String query) {
        var dialect = CURRENT_DIALECT.get();
        var cleaned = query.strip().replaceAll(";\\s*$", "");

        try {
            var settings = new Settings().withParseUnknownFunctions(ParseUnknownFunctions.IGNORE);
            var parsed = DSL.using(dialect, settings).parser().parseQuery(cleaned);
            return DSL.using(dialect, settings).render(parsed);
        } catch (DataAccessException e) {
            throw new IllegalArgumentException("Failed to parse SQL query for source: %s".formatted(e.getMessage()), e);
        }
    }

    // --- Field compilation ---

    private static List<ExpressionField> resolveExpressionFields(Set<Field> viewFields, Set<String> projectedFields) {
        var expressionFields = viewFields.stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .toList();

        if (projectedFields.isEmpty()) {
            return expressionFields;
        }

        return expressionFields.stream()
                .filter(f -> isExpressionFieldProjected(f, projectedFields))
                .toList();
    }

    private static boolean isExpressionFieldProjected(ExpressionField field, Set<String> projectedFields) {
        var fieldName = field.getFieldName();
        if (projectedFields.contains(fieldName)) {
            return true;
        }
        var prefix = fieldName + ".";
        return projectedFields.stream().anyMatch(p -> p.startsWith(prefix));
    }

    private static List<SelectField<?>> compileFieldSelects(List<ExpressionField> fields) {
        var selects = new ArrayList<SelectField<?>>();
        for (var field : fields) {
            selects.add(compileFieldExpression(field));
        }
        return selects;
    }

    private static SelectField<?> compileFieldExpression(ExpressionField exprField) {
        var fieldName = exprField.getFieldName();

        var reference = exprField.getReference();
        if (reference != null) {
            return DSL.field("{0}", field(quotedName(CTE_ALIAS, reference))).as(fieldAlias(fieldName));
        }

        var template = exprField.getTemplate();
        if (template != null) {
            return compileTemplateExpression(template, fieldName);
        }

        var constant = exprField.getConstant();
        if (constant != null) {
            return inline(constant.stringValue()).as(fieldAlias(fieldName));
        }

        if (exprField.getFunctionValue() != null || exprField.getFunctionExecution() != null) {
            throw new UnsupportedOperationException(
                    "Function-based field expressions are not supported in reactive SQL compilation: %s"
                            .formatted(fieldName));
        }

        throw new IllegalArgumentException(
                "ExpressionField [%s] has no reference, template, or constant defined".formatted(fieldName));
    }

    private static SelectField<?> compileTemplateExpression(Template template, String fieldName) {
        var segments = template.getSegments();
        var parts = segments.stream()
                .map(segment -> {
                    if (segment instanceof CarmlTemplate.ExpressionSegment) {
                        return field(quotedName(CTE_ALIAS, segment.getValue()));
                    }
                    return inline(segment.getValue());
                })
                .toList();

        if (parts.size() == 1) {
            return parts.get(0).as(fieldAlias(fieldName));
        }

        var concatParts = parts.toArray(org.jooq.Field[]::new);
        return DSL.concat(concatParts).as(fieldAlias(fieldName));
    }

    // --- Join compilation ---

    private static List<JoinDescriptor> compileJoins(
            LogicalView view,
            DSLContext ctx,
            Set<StructuralAnnotation> annotations,
            Set<String> notNullFieldNames,
            Set<String> projectedFields,
            Set<LogicalViewJoin> aggregatingJoins) {
        var joinDescriptors = new ArrayList<JoinDescriptor>();
        int parentIndex = 0;

        for (var viewJoin : view.getLeftJoins()) {
            if (canEliminateJoin(viewJoin, annotations, projectedFields)) {
                continue;
            }
            var effectivelyLeftJoin = !canUpgradeToInnerJoin(viewJoin, notNullFieldNames);
            joinDescriptors.add(compileJoinDescriptor(
                    viewJoin, ctx, parentIndex++, effectivelyLeftJoin, aggregatingJoins.contains(viewJoin)));
        }

        for (var viewJoin : view.getInnerJoins()) {
            if (canEliminateJoin(viewJoin, annotations, projectedFields)) {
                continue;
            }
            joinDescriptors.add(
                    compileJoinDescriptor(viewJoin, ctx, parentIndex++, false, aggregatingJoins.contains(viewJoin)));
        }

        return joinDescriptors;
    }

    private static JoinDescriptor compileJoinDescriptor(
            LogicalViewJoin viewJoin, DSLContext ctx, int parentIndex, boolean isLeftJoin, boolean isAggregating) {
        var parentView = viewJoin.getParentLogicalView();
        var parentAlias = PARENT_ALIAS_PREFIX + parentIndex;

        var parentSql = doCompileWithCycleDetection(parentView, EvaluationContext.defaults());

        if (isAggregating) {
            return compileAggregatingJoinDescriptor(viewJoin, ctx, parentSql, parentAlias, isLeftJoin);
        }

        return buildJoinDescriptor(viewJoin, parentSql, parentAlias, isLeftJoin);
    }

    private static JoinDescriptor compileAggregatingJoinDescriptor(
            LogicalViewJoin viewJoin, DSLContext ctx, String parentSql, String parentAlias, boolean isLeftJoin) {
        var groupByRefs = viewJoin.getJoinConditions().stream()
                .map(join -> join.getParentMap().getReference())
                .sorted()
                .distinct()
                .toList();

        var projectedRefs = viewJoin.getFields().stream()
                .map(ExpressionField::getReference)
                .filter(Objects::nonNull)
                .sorted()
                .distinct()
                .toList();

        var aggAlias = "__agg";
        var orderByField = field(quotedName(aggAlias, INDEX_COLUMN));
        var aggSelects = new ArrayList<SelectField<?>>();
        for (var groupByRef : groupByRefs) {
            aggSelects.add(field(quotedName(aggAlias, groupByRef)));
        }
        for (var projRef : projectedRefs) {
            var projField = field(quotedName(aggAlias, projRef));
            // jOOQ's arrayAgg translates to array_agg for PostgreSQL and json_arrayagg for MySQL.
            // Note: MySQL json_arrayagg returns a JSON array string, not a native array type.
            // If aggregating joins on MySQL need proper array decomposition, this will require
            // MySQL-specific handling in the evaluator.
            aggSelects.add(DSL.arrayAgg(projField).orderBy(orderByField).as(quotedName(projRef)));
        }

        var groupByFields = groupByRefs.stream()
                .map(ref -> field(quotedName(aggAlias, ref)))
                .toArray(org.jooq.Field[]::new);

        var aggQuery = ctx.select(aggSelects)
                .from(table(SUBQUERY_TEMPLATE, sql(parentSql)).as(quotedName(aggAlias)))
                .groupBy(groupByFields);

        return buildJoinDescriptor(viewJoin, aggQuery.getSQL(), parentAlias, isLeftJoin);
    }

    private static JoinDescriptor buildJoinDescriptor(
            LogicalViewJoin viewJoin, String parentSql, String parentAlias, boolean isLeftJoin) {
        var parentTable = table(SUBQUERY_TEMPLATE, sql(parentSql)).as(quotedName(parentAlias));
        var onCondition = buildJoinCondition(viewJoin.getJoinConditions(), parentAlias);

        var joinFields = new ArrayList<SelectField<?>>();
        for (var f : viewJoin.getFields()) {
            joinFields.add(compileJoinFieldExpression(parentAlias, f));
        }

        return new JoinDescriptor(parentTable, onCondition, joinFields, isLeftJoin);
    }

    private static Condition buildJoinCondition(Set<Join> joinConditions, String parentAlias) {
        return joinConditions.stream()
                .map(join -> {
                    var childRef = join.getChildMap().getReference();
                    var parentRef = join.getParentMap().getReference();
                    var childField = field(quotedName(CTE_ALIAS, childRef));
                    return childField.eq(field(quotedName(parentAlias, parentRef)));
                })
                .reduce(Condition::and)
                .orElseThrow(() -> new IllegalArgumentException("LogicalViewJoin has no join conditions"));
    }

    private static SelectField<?> compileJoinFieldExpression(String parentAlias, ExpressionField joinField) {
        var fieldName = joinField.getFieldName();
        var reference = joinField.getReference();

        if (reference != null) {
            return field(quotedName(parentAlias, reference)).as(fieldAlias(fieldName));
        }

        throw new UnsupportedOperationException(
                "Join projected field [%s] must have a reference expression".formatted(fieldName));
    }

    // --- FROM clause and SELECT helpers ---

    private static <T extends org.jooq.Record> SelectJoinStep<T> buildFromClause(
            SelectFromStep<T> selectStep, List<JoinDescriptor> joinDescriptors) {
        var fromStep = selectStep.from(table(name(CTE_ALIAS)));
        return applyJoins(fromStep, joinDescriptors);
    }

    private static <T extends org.jooq.Record> SelectJoinStep<T> applyJoins(
            SelectJoinStep<T> fromStep, List<JoinDescriptor> joinDescriptors) {
        var current = fromStep;
        for (var joinDesc : joinDescriptors) {
            if (joinDesc.isLeftJoin()) {
                current = current.leftJoin(joinDesc.table()).on(joinDesc.condition());
            } else {
                current = current.join(joinDesc.table()).on(joinDesc.condition());
            }
        }
        return current;
    }

    private static List<SelectField<?>> collectAllFieldSelects(
            List<ExpressionField> expressionFields, List<JoinDescriptor> joinDescriptors) {
        var allSelects = new ArrayList<>(compileFieldSelects(expressionFields));
        for (var joinDesc : joinDescriptors) {
            allSelects.addAll(joinDesc.fields());
        }
        return allSelects;
    }

    // --- Structural annotation optimization helpers ---

    private static Set<String> extractNotNullFieldNames(Set<StructuralAnnotation> annotations) {
        if (annotations == null || annotations.isEmpty()) {
            return Set.of();
        }

        return annotations.stream()
                .filter(NotNullAnnotation.class::isInstance)
                .flatMap(annotation -> annotation.getOnFields().stream())
                .map(Field::getFieldName)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static boolean canSkipDistinct(
            Set<StructuralAnnotation> annotations,
            Set<String> notNullFieldNames,
            LogicalView view,
            EvaluationContext context) {
        if (annotations == null || annotations.isEmpty()) {
            return false;
        }

        var selectedFields = resolveSelectedFieldNames(view, context);
        return hasCoveringUniqueConstraint(annotations, notNullFieldNames, selectedFields);
    }

    private static Set<String> resolveSelectedFieldNames(LogicalView view, EvaluationContext context) {
        var projectedFields = context.getProjectedFields();
        if (!projectedFields.isEmpty()) {
            return projectedFields;
        }

        return view.getFields().stream()
                .flatMap(field -> {
                    if (field instanceof IterableField iterableField) {
                        return iterableField.getFields().stream().map(Field::getFieldName);
                    }
                    return Stream.of(field.getFieldName());
                })
                .collect(Collectors.toUnmodifiableSet());
    }

    private static boolean hasCoveringUniqueConstraint(
            Set<StructuralAnnotation> annotations, Set<String> notNullFieldNames, Set<String> selectedFields) {
        for (var annotation : annotations) {
            if (annotation instanceof PrimaryKeyAnnotation) {
                var pkFieldNames = annotation.getOnFields().stream()
                        .map(Field::getFieldName)
                        .collect(Collectors.toUnmodifiableSet());

                if (!pkFieldNames.isEmpty() && selectedFields.containsAll(pkFieldNames)) {
                    LOG.debug("PrimaryKey annotation covers selected fields {} - omitting DISTINCT", pkFieldNames);
                    return true;
                }
            }

            if (annotation instanceof UniqueAnnotation) {
                var uniqueFieldNames = annotation.getOnFields().stream()
                        .map(Field::getFieldName)
                        .collect(Collectors.toUnmodifiableSet());

                if (!uniqueFieldNames.isEmpty()
                        && notNullFieldNames.containsAll(uniqueFieldNames)
                        && selectedFields.containsAll(uniqueFieldNames)) {
                    LOG.debug(
                            "Unique+NotNull annotation covers selected fields {} - omitting DISTINCT",
                            uniqueFieldNames);
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean canEliminateJoin(
            LogicalViewJoin viewJoin, Set<StructuralAnnotation> annotations, Set<String> projectedFields) {
        if (annotations == null || annotations.isEmpty()) {
            return false;
        }

        var parentView = viewJoin.getParentLogicalView();
        var joinProjectedFieldNames = viewJoin.getFields().stream()
                .map(ExpressionField::getFieldName)
                .collect(Collectors.toUnmodifiableSet());

        boolean parentFieldsNotProjected;
        if (joinProjectedFieldNames.isEmpty()) {
            parentFieldsNotProjected = true;
        } else if (projectedFields.isEmpty()) {
            parentFieldsNotProjected = false;
        } else {
            parentFieldsNotProjected = joinProjectedFieldNames.stream().noneMatch(projectedFields::contains);
        }

        if (!parentFieldsNotProjected) {
            return false;
        }

        for (var annotation : annotations) {
            if (annotation instanceof ForeignKeyAnnotation fkAnnotation && fkAnnotation.getTargetView() == parentView) {
                LOG.debug(
                        "ForeignKey annotation to parent view [{}] with no projected parent fields - eliminating JOIN",
                        parentView.getResourceName());
                return true;
            }
        }

        return false;
    }

    private static boolean canUpgradeToInnerJoin(LogicalViewJoin viewJoin, Set<String> notNullFieldNames) {
        if (notNullFieldNames.isEmpty()) {
            return false;
        }

        var childJoinKeyRefs = viewJoin.getJoinConditions().stream()
                .map(join -> join.getChildMap().getReference())
                .collect(Collectors.toUnmodifiableSet());

        if (notNullFieldNames.containsAll(childJoinKeyRefs)) {
            LOG.debug(
                    "NotNull annotation covers child join keys {} - upgrading LEFT JOIN to INNER JOIN",
                    childJoinKeyRefs);
            return true;
        }

        return false;
    }

    static org.jooq.Name fieldAlias(String alias) {
        if (alias.matches("[a-zA-Z_]\\w*")) {
            return name(alias);
        }
        return quotedName(alias);
    }
}
