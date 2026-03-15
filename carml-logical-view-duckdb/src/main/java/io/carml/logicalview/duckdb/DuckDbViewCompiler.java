package io.carml.logicalview.duckdb;

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
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Compiles a {@link LogicalView} and {@link EvaluationContext} into a DuckDB SQL query string.
 *
 * <p>This is a pure function class with no side effects and no DuckDB connection. It produces
 * CTE-structured SQL using WITH clauses for view composition.
 *
 * <p>Uses jOOQ's type-safe DSL for SQL construction with {@link SQLDialect#DUCKDB}.
 *
 * <p>Supports expression fields (mapped to SELECT columns), iterable fields (UNNEST cross-joins),
 * logical view joins (LEFT JOIN / INNER JOIN with recursive parent view compilation), and
 * structural annotation-based optimizations (DISTINCT elimination, JOIN elimination, LEFT to INNER
 * JOIN upgrade).
 *
 * <p>For JSON sources with iterators, uses {@code json_extract} + {@code UNNEST} to expand the
 * iterator path into rows, and {@code json_extract_string} to extract field values from the JSON
 * rows. JSONPath filter expressions are translated to SQL WHERE clauses via
 * {@link JsonPathAnalyzer}.
 *
 * <p><b>IriSafe annotation note:</b> {@code IriSafeAnnotation} does not currently affect SQL
 * compilation. Templates already use raw {@code CONCAT} without percent-encoding. When IRI encoding
 * support is added to the DuckDB evaluator (e.g., via a UDF), IriSafe annotations should be used
 * to skip that encoding step for annotated fields.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DuckDbViewCompiler {

    static final String INDEX_COLUMN = "__idx";

    private static final String CTE_ALIAS = "view_source";

    private static final String DEDUPED_ALIAS = "deduped";

    private static final String PARENT_ALIAS_PREFIX = "parent_";

    private static final DSLContext CTX = DSL.using(SQLDialect.DUCKDB);

    /**
     * Tracks views currently being compiled on the current thread. Used to detect cycles in
     * view-on-view composition (e.g., view1 -> view2 -> view1). Each recursive {@link #compile}
     * call adds the view to this set; it is removed when compilation completes.
     */
    private static final ThreadLocal<Set<LogicalView>> COMPILING_VIEWS = ThreadLocal.withInitial(HashSet::new);

    /**
     * Cached class reference for the none dedup strategy, used to detect whether deduplication
     * should be applied. The concrete class is package-private in {@code io.carml.logicalview},
     * so we resolve it once via the public factory method.
     */
    private static final Class<? extends DedupStrategy> NONE_DEDUP_CLASS =
            DedupStrategy.none().getClass();

    /**
     * Describes an UNNEST cross-join derived from an {@link IterableField}.
     *
     * @param unnestTable the jOOQ table expression for the UNNEST
     * @param nestedSelects the SELECT fields for the nested expression fields
     */
    private record UnnestDescriptor(Table<?> unnestTable, List<SelectField<?>> nestedSelects) {}

    /**
     * Describes a SQL JOIN derived from a {@link LogicalViewJoin}.
     *
     * @param table the jOOQ subquery table for the parent view
     * @param condition the ON condition for the join
     * @param fields the SELECT fields projected from the parent
     * @param isLeftJoin {@code true} for LEFT JOIN, {@code false} for INNER JOIN
     */
    private record JoinDescriptor(
            Table<?> table, Condition condition, List<SelectField<?>> fields, boolean isLeftJoin) {}

    /**
     * Compiles a {@link LogicalView} into a DuckDB SQL query string.
     *
     * <p>Applies structural annotation optimizations:
     * <ul>
     *   <li>PrimaryKey or Unique+NotNull covering selected fields: omits DISTINCT</li>
     *   <li>ForeignKey with no projected parent fields: eliminates JOIN entirely</li>
     *   <li>NotNull on child join keys: upgrades LEFT JOIN to INNER JOIN</li>
     * </ul>
     *
     * @param view the logical view defining fields and the underlying data source
     * @param context the evaluation context controlling projection, dedup, and limits
     * @return a DuckDB-compatible SQL query string
     * @throws IllegalArgumentException if the view's source type is unsupported or the view
     *     structure cannot be compiled
     */
    public static String compile(LogicalView view, EvaluationContext context) {
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
        var viewOn = view.getViewOn();

        DuckDbSourceStrategy strategy;
        String sourceTable;

        if (viewOn instanceof LogicalView innerView) {
            // Recursively compile the inner view and wrap its SQL as a subquery.
            // The inner view produces a complete CTE query (WITH ... SELECT ...),
            // so it must be parenthesized to be used as a derived table in FROM.
            // ColumnSourceStrategy is used because the inner view exposes named columns.
            var innerSql = compile(innerView, EvaluationContext.defaults());
            sourceTable = "(%s)".formatted(innerSql);
            strategy = new ColumnSourceStrategy(CTE_ALIAS, ColumnSourceStrategy.TypeCompanionMode.INNER_VIEW);
        } else if (viewOn instanceof LogicalSource logicalSource) {
            var compiledSource = compileSourceClause(logicalSource, view);
            strategy = compiledSource.strategy();
            sourceTable = compiledSource.sourceSql();
        } else {
            throw new IllegalArgumentException("Unsupported viewOn target type: %s"
                    .formatted(viewOn.getClass().getName()));
        }

        var expressionFields = resolveExpressionFields(view.getFields(), context.getProjectedFields(), strategy);
        var iterableUnnestDescriptors =
                resolveUnnestDescriptors(view.getFields(), context.getProjectedFields(), strategy);
        var multiValuedUnnestDescriptors =
                resolveMultiValuedUnnestDescriptors(view.getFields(), context.getProjectedFields(), strategy);
        var unnestDescriptors = new ArrayList<>(iterableUnnestDescriptors);
        unnestDescriptors.addAll(multiValuedUnnestDescriptors);

        // Extract structural annotations for optimization decisions
        var annotations = view.getStructuralAnnotations();
        var notNullFieldNames = extractNotNullFieldNames(annotations);

        var joinDescriptors =
                compileJoins(view, annotations, notNullFieldNames, context.getProjectedFields(), strategy);

        // Determine whether DISTINCT is needed, considering annotation-based optimization
        var dedupRequested = !NONE_DEDUP_CLASS.isInstance(context.getDedupStrategy());
        var useDistinct = dedupRequested && !canSkipDistinct(annotations, notNullFieldNames, view, context);

        // ROW_NUMBER() is computed inside the CTE so that each source row gets a stable index
        // that is preserved through UNNEST expansion and JOIN multiplication.
        var viewSourceCte = name(CTE_ALIAS)
                .as(CTX.select(asterisk(), rowNumber().over().as(name(INDEX_COLUMN)))
                        .from(sql(sourceTable)));

        String result;
        var allFieldSelects = collectAllFieldSelects(expressionFields, unnestDescriptors, joinDescriptors, strategy);

        if (useDistinct) {
            var dedupFrom = buildFromClause(
                    CTX.selectDistinct(allFieldSelects.toArray(SelectField[]::new)),
                    unnestDescriptors,
                    joinDescriptors);

            var dedupedCte = name(DEDUPED_ALIAS).as(dedupFrom);

            var outerQuery = CTX.with(viewSourceCte)
                    .with(dedupedCte)
                    .select(asterisk(), rowNumber().over().as(name(INDEX_COLUMN)))
                    .from(table(name(DEDUPED_ALIAS)));

            result = context.getLimit()
                    .map(limit -> outerQuery.limit(limit).getSQL())
                    .orElseGet(outerQuery::getSQL);
        } else {
            // Reference the pre-computed index from the CTE
            allFieldSelects.add(field(quotedName(CTE_ALIAS, INDEX_COLUMN)));

            // Include raw JSON iterator column for source-level expression evaluation.
            // When retainSourceEvaluation is true (implicit views), gather map expressions that
            // are NOT view fields need to be evaluated from the raw JSON data. The __iter column
            // carries the raw JSON for each iteration row, enabling DuckDbJsonSourceEvaluation to
            // evaluate these expressions using JSONPath at mapping time.
            // This is only added in the non-DISTINCT path because the raw JSON blob would interfere
            // with deduplication (two rows with identical field values but different JSON formatting
            // would survive DISTINCT).
            if (context.retainSourceEvaluation() && strategy instanceof JsonIteratorSourceStrategy) {
                allFieldSelects.add(field(quotedName(CTE_ALIAS, JsonPathSourceHandler.JSON_ITER_COLUMN))
                        .as(quotedName(JsonPathSourceHandler.JSON_ITER_COLUMN)));
            }

            var fromStep = buildFromClause(
                    CTX.with(viewSourceCte).select(allFieldSelects), unnestDescriptors, joinDescriptors);

            result = context.getLimit()
                    .map(limit -> fromStep.limit(limit).getSQL())
                    .orElseGet(fromStep::getSQL);
        }

        LOG.debug("Compiled DuckDB SQL for view [{}]:\n{}", view.getResourceName(), result);
        return result;
    }

    // --- Structural annotation optimization helpers ---

    /**
     * Extracts the set of field names that are covered by {@link NotNullAnnotation}s.
     *
     * @param annotations the structural annotations declared on the view
     * @return a set of field names guaranteed to be non-null
     */
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

    /**
     * Determines whether DISTINCT can be omitted based on structural annotations. DISTINCT can be
     * skipped when a {@link PrimaryKeyAnnotation} or a {@link UniqueAnnotation} (with all its
     * fields also covered by {@link NotNullAnnotation}) covers all the fields being selected.
     *
     * <p>"Covers" means every field name in the PK/Unique annotation is present in the set of
     * fields being selected, or the projection is empty (meaning all fields are selected).
     *
     * @param annotations the structural annotations declared on the view
     * @param notNullFieldNames the set of field names known to be non-null
     * @param view the logical view
     * @param context the evaluation context
     * @return {@code true} if DISTINCT can be safely omitted
     */
    private static boolean canSkipDistinct(
            Set<StructuralAnnotation> annotations,
            Set<String> notNullFieldNames,
            LogicalView view,
            EvaluationContext context) {
        if (annotations == null || annotations.isEmpty()) {
            return false;
        }

        // Determine the set of field names being selected
        var selectedFields = resolveSelectedFieldNames(view, context);

        return hasCoveringUniqueConstraint(annotations, notNullFieldNames, selectedFields);
    }

    /**
     * Resolves the set of field names that will appear in the SELECT clause. If projection is empty
     * (all fields), collects all field names from the view's fields. Otherwise, returns the
     * projected field names.
     */
    private static Set<String> resolveSelectedFieldNames(LogicalView view, EvaluationContext context) {
        var projectedFields = context.getProjectedFields();
        if (!projectedFields.isEmpty()) {
            return projectedFields;
        }

        // All fields are selected: collect all field names including nested fields
        return view.getFields().stream()
                .flatMap(field -> {
                    if (field instanceof IterableField iterableField) {
                        return iterableField.getFields().stream().map(Field::getFieldName);
                    }
                    return Stream.of(field.getFieldName());
                })
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Checks whether any PrimaryKey annotation or Unique+NotNull annotation combination covers
     * (i.e., is a subset of) the selected fields. If so, uniqueness is guaranteed and DISTINCT
     * is unnecessary.
     *
     * @param annotations the structural annotations
     * @param notNullFieldNames field names covered by NotNull annotations
     * @param selectedFields the field names being selected
     * @return {@code true} if a covering unique constraint exists
     */
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

                // Unique is only sufficient for dedup skip when all unique fields are also NotNull
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

    /**
     * Determines whether a join can be eliminated entirely. A join can be eliminated when a
     * {@link ForeignKeyAnnotation} on the child view points to the join's parent view, AND none
     * of the join's projected fields appear in the context's projected fields (or the join has no
     * projected fields). The FK guarantees referential integrity, so the join does not affect
     * cardinality when no parent fields are needed.
     *
     * @param viewJoin the logical view join to check
     * @param annotations the structural annotations on the child view
     * @param projectedFields the context's projected fields (empty means all)
     * @return {@code true} if the join can be eliminated
     */
    private static boolean canEliminateJoin(
            LogicalViewJoin viewJoin, Set<StructuralAnnotation> annotations, Set<String> projectedFields) {
        if (annotations == null || annotations.isEmpty()) {
            return false;
        }

        var parentView = viewJoin.getParentLogicalView();
        var joinProjectedFieldNames = viewJoin.getFields().stream()
                .map(ExpressionField::getFieldName)
                .collect(Collectors.toUnmodifiableSet());

        // If the join has no projected fields, it's a candidate for elimination
        // If it has projected fields but none are in the context projection, also a candidate
        boolean parentFieldsNotProjected;
        if (joinProjectedFieldNames.isEmpty()) {
            parentFieldsNotProjected = true;
        } else if (projectedFields.isEmpty()) {
            // Empty projection means "all fields" — parent fields ARE projected
            parentFieldsNotProjected = false;
        } else {
            parentFieldsNotProjected = joinProjectedFieldNames.stream().noneMatch(projectedFields::contains);
        }

        if (!parentFieldsNotProjected) {
            return false;
        }

        // Check if there is a ForeignKeyAnnotation pointing to this join's parent view
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

    /**
     * Determines whether a LEFT JOIN can be upgraded to an INNER JOIN. This is possible when all
     * child-side join key fields are covered by {@link NotNullAnnotation}s. When child keys are
     * guaranteed non-null, every child row either matches or doesn't match in the parent, which
     * produces identical results for LEFT JOIN and INNER JOIN. INNER JOIN allows better query
     * optimization by DuckDB.
     *
     * @param viewJoin the logical view join to check
     * @param notNullFieldNames the set of field names known to be non-null
     * @return {@code true} if the LEFT JOIN can be upgraded to INNER JOIN
     */
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

    // --- SQL compilation methods ---

    /**
     * Builds the FROM clause including the base view_source table, any UNNEST cross-joins, and any
     * JOIN clauses from logical view joins.
     */
    private static <T extends org.jooq.Record> org.jooq.SelectJoinStep<T> buildFromClause(
            org.jooq.SelectFromStep<T> selectStep,
            List<UnnestDescriptor> unnestDescriptors,
            List<JoinDescriptor> joinDescriptors) {
        // Start with the base table. If there are unnest descriptors, cross join them.
        if (unnestDescriptors.isEmpty()) {
            var fromStep = selectStep.from(table(name(CTE_ALIAS)));
            return applyJoins(fromStep, joinDescriptors);
        }

        // Build comma-separated FROM: "view_source", unnest(...) AS alias, ...
        var tables = new ArrayList<Table<?>>();
        tables.add(table(name(CTE_ALIAS)));
        for (var unnest : unnestDescriptors) {
            tables.add(unnest.unnestTable());
        }

        var fromStep = selectStep.from(tables);
        return applyJoins(fromStep, joinDescriptors);
    }

    /**
     * Applies JOIN clauses (left or inner) to a select-join step.
     */
    private static <T extends org.jooq.Record> org.jooq.SelectJoinStep<T> applyJoins(
            org.jooq.SelectJoinStep<T> fromStep, List<JoinDescriptor> joinDescriptors) {
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

    /**
     * Collects all SELECT field expressions from expression fields, unnest descriptors, and join
     * descriptors into a single mutable list.
     */
    private static List<SelectField<?>> collectAllFieldSelects(
            List<ExpressionField> expressionFields,
            List<UnnestDescriptor> unnestDescriptors,
            List<JoinDescriptor> joinDescriptors,
            DuckDbSourceStrategy strategy) {
        var allSelects = new ArrayList<>(compileFieldSelects(expressionFields, strategy));
        for (var unnest : unnestDescriptors) {
            allSelects.addAll(unnest.nestedSelects());
        }
        for (var joinDesc : joinDescriptors) {
            allSelects.addAll(joinDesc.fields());
        }
        return allSelects;
    }

    private static DuckDbSourceHandler.CompiledSource compileSourceClause(
            LogicalSource logicalSource, LogicalView view) {
        var refFormulation = logicalSource.getReferenceFormulation();
        if (refFormulation == null) {
            throw new IllegalArgumentException("LogicalSource has no reference formulation");
        }

        var refIri = refFormulation.getAsResource();
        return DuckDbSourceHandler.forFormulation(refIri)
                .orElseThrow(
                        () -> new IllegalArgumentException("Unsupported reference formulation: %s".formatted(refIri)))
                .compileSource(logicalSource, view.getFields(), CTE_ALIAS);
    }

    /**
     * Resolves the top-level {@link ExpressionField} instances to include in the SELECT clause,
     * excluding multi-valued fields which are handled as UNNESTs. If projected fields is non-empty,
     * only fields whose names match the projection are included.
     */
    private static List<ExpressionField> resolveExpressionFields(
            Set<Field> viewFields, Set<String> projectedFields, DuckDbSourceStrategy strategy) {
        var expressionFields = viewFields.stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .filter(f -> !isMultiValuedExpressionField(f, strategy))
                .toList();

        if (projectedFields.isEmpty()) {
            return expressionFields;
        }

        return expressionFields.stream()
                .filter(f -> projectedFields.contains(f.getFieldName()))
                .toList();
    }

    /**
     * Checks whether an expression field has a multi-valued reference that requires UNNEST
     * expansion.
     */
    private static boolean isMultiValuedExpressionField(ExpressionField field, DuckDbSourceStrategy strategy) {
        var reference = field.getReference();
        return reference != null && strategy.isMultiValuedReference(reference);
    }

    /**
     * Resolves multi-valued {@link ExpressionField} instances and produces {@link UnnestDescriptor}s
     * for UNNEST cross-joins. Multi-valued fields are those whose reference evaluates to multiple
     * values (e.g., {@code $.items[*]} in JSONPath), requiring row expansion via UNNEST.
     */
    private static List<UnnestDescriptor> resolveMultiValuedUnnestDescriptors(
            Set<Field> viewFields, Set<String> projectedFields, DuckDbSourceStrategy strategy) {
        var multiValuedFields = viewFields.stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .filter(f -> isMultiValuedExpressionField(f, strategy))
                .toList();

        if (!projectedFields.isEmpty()) {
            multiValuedFields = multiValuedFields.stream()
                    .filter(f -> projectedFields.contains(f.getFieldName()))
                    .toList();
        }

        return multiValuedFields.stream()
                .map(f -> compileMultiValuedUnnestDescriptor(f, strategy))
                .toList();
    }

    /**
     * Compiles a multi-valued {@link ExpressionField} into an {@link UnnestDescriptor}. The field's
     * reference is used as an iterator expression for UNNEST, and the unnested value is extracted
     * using the strategy's nested field reference compilation with root path {@code "$"}.
     *
     * <p>When the reference contains JSONPath filter expressions (e.g.,
     * {@code $.items[?(@.active==true)]}), the UNNEST table is compiled using the normalized base
     * path ({@code $.items[*]}) and wrapped with a WHERE clause that applies the filter conditions
     * to the unnested values. Ordinals are recomputed after filtering to produce sequential 0-based
     * indices.
     */
    private static UnnestDescriptor compileMultiValuedUnnestDescriptor(
            ExpressionField field, DuckDbSourceStrategy strategy) {
        var fieldName = field.getFieldName();
        var reference = field.getReference();

        var parsed = JsonPathAnalyzer.analyze(reference);

        Table<?> unnestTable;
        if (parsed.filters().isEmpty()) {
            unnestTable = strategy.compileUnnestTable(reference, CTE_ALIAS, true, fieldName);
        } else {
            // Build a filtered LATERAL: unnest all elements using the basePath, then apply a WHERE
            // clause for the filter conditions and recompute ordinals as sequential 0-based indices.
            //
            // The inner unnest expands all array elements. The outer LATERAL wraps it with a WHERE
            // clause to apply the filter, and row_number() to produce sequential ordinals.
            var innerUnnest = strategy.compileUnnestTable(parsed.basePath(), CTE_ALIAS, true, fieldName + "_inner");

            var filterCondition = parsed.filters().stream()
                    .map(f -> JsonPathSourceHandler.compileFilterCondition(f, JsonIteratorSourceStrategy.UNNEST_FIELD))
                    .reduce(Condition::and)
                    .orElseThrow();

            // Render the inner unnest as a subquery in a SELECT to capture its full SQL,
            // then wrap with WHERE and ordinal recomputation. row_number() over() has no ORDER BY
            // because DuckDB's UNNEST preserves array element order, and the LATERAL boundary
            // resets numbering per parent row.
            var innerQuery = CTX.renderInlined(CTX.selectFrom(innerUnnest));

            unnestTable = table("LATERAL (SELECT \"unnest\", (row_number() over() - 1) AS \"%s\" FROM (%s) WHERE %s)"
                            .formatted(
                                    DuckDbSourceStrategy.ORDINAL_FIELD, innerQuery, CTX.renderInlined(filterCondition)))
                    .as(quotedName(fieldName));
        }

        var nestedSelects = new ArrayList<SelectField<?>>();

        // Extract value from unnested element: json_extract_string(fieldName."unnest", '$')
        nestedSelects.add(strategy.compileNestedFieldReference(fieldName, "$", fieldAlias(fieldName)));

        // Add type companion for the unnested value
        var typeAlias = quotedName(fieldName + DuckDbSourceStrategy.TYPE_SUFFIX);
        nestedSelects.add(strategy.compileNestedFieldTypeReference(fieldName, "$", typeAlias));

        // Add ordinal column: fieldName."__ord" AS "fieldName.#"
        var indexColumnName = fieldName + ".#";
        nestedSelects.add(
                field(quotedName(fieldName, DuckDbSourceStrategy.ORDINAL_FIELD)).as(quotedName(indexColumnName)));

        return new UnnestDescriptor(unnestTable, nestedSelects);
    }

    /**
     * Resolves {@link IterableField} instances from the view fields and produces
     * {@link UnnestDescriptor}s for UNNEST cross-joins. Recursively processes nested iterable
     * fields, producing one descriptor per iterable level.
     *
     * <p>If projected fields is non-empty, only nested fields whose prefixed names match the
     * projection are included.
     */
    private static List<UnnestDescriptor> resolveUnnestDescriptors(
            Set<Field> viewFields, Set<String> projectedFields, DuckDbSourceStrategy strategy) {
        var result = new ArrayList<UnnestDescriptor>();
        collectUnnestDescriptors(viewFields, projectedFields, strategy, "", result);
        return result;
    }

    /**
     * Recursively collects {@link UnnestDescriptor}s from the field hierarchy. Each
     * {@link IterableField} generates one descriptor for its own UNNEST and nested expression
     * fields, then recurses into any nested iterable fields.
     *
     * @param fields the fields to process
     * @param projectedFields the context's projected fields
     * @param strategy the source strategy for field access compilation
     * @param prefix the dot-separated prefix for absolute field names (e.g., "item.")
     * @param result the accumulator for descriptors
     */
    private static void collectUnnestDescriptors(
            Set<Field> fields,
            Set<String> projectedFields,
            DuckDbSourceStrategy strategy,
            String prefix,
            List<UnnestDescriptor> result) {
        for (var field : fields) {
            if (field instanceof IterableField iterableField) {
                var absoluteName = prefix + iterableField.getFieldName();
                var descriptor =
                        compileUnnestDescriptor(iterableField, projectedFields, strategy, prefix, absoluteName);
                result.add(descriptor);

                // Recursively process nested iterable fields
                collectUnnestDescriptors(
                        iterableField.getFields(), projectedFields, strategy, absoluteName + ".", result);
            }
        }
    }

    /**
     * Compiles a single {@link IterableField} into an {@link UnnestDescriptor} containing the
     * UNNEST table expression and the nested SELECT fields. Delegates to the source strategy for
     * table and field expression compilation.
     *
     * @param iterableField the iterable field to compile
     * @param projectedFields the context's projected fields
     * @param strategy the source strategy for field access compilation
     * @param prefix the dot-separated prefix for absolute nested field names
     * @param absoluteName the absolute name of this iterable field (used as table alias)
     */
    private static UnnestDescriptor compileUnnestDescriptor(
            IterableField iterableField,
            Set<String> projectedFields,
            DuckDbSourceStrategy strategy,
            String prefix,
            String absoluteName) {
        var iterator = iterableField.getIterator();

        if (iterator == null || iterator.isBlank()) {
            throw new IllegalArgumentException(
                    "IterableField [%s] has no iterator expression defined".formatted(absoluteName));
        }

        var parentAlias = prefix.isEmpty() ? CTE_ALIAS : prefix.substring(0, prefix.length() - 1);
        var unnestTable = strategy.compileUnnestTable(iterator, parentAlias, prefix.isEmpty(), absoluteName);

        // Resolve nested expression fields (not nested iterable fields — those are handled recursively)
        var nestedFields = iterableField.getFields().stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .toList();

        var nestedPrefix = absoluteName + ".";
        var filteredNested = projectedFields.isEmpty()
                ? nestedFields
                : nestedFields.stream()
                        .filter(f -> projectedFields.contains(nestedPrefix + f.getFieldName()))
                        .toList();

        // Build SELECT fields qualified by the unnest alias, with type companions
        var nestedSelects = new ArrayList<SelectField<?>>();
        for (var nested : filteredNested) {
            nestedSelects.add(compileNestedFieldExpression(absoluteName, nested, strategy, nestedPrefix));
            // Add type companion for reference-based nested fields
            if (nested.getReference() != null) {
                var absoluteFieldName = nestedPrefix + nested.getFieldName();
                var typeAlias = quotedName(absoluteFieldName + DuckDbSourceStrategy.TYPE_SUFFIX);
                nestedSelects.add(
                        strategy.compileNestedFieldTypeReference(absoluteName, nested.getReference(), typeAlias));
            }
        }

        // Add iterable field ordinal column (0-based index within each parent's unnested array)
        var indexColumnName = absoluteName + ".#";
        nestedSelects.add(field(quotedName(absoluteName, DuckDbSourceStrategy.ORDINAL_FIELD))
                .as(quotedName(indexColumnName)));

        return new UnnestDescriptor(unnestTable, nestedSelects);
    }

    /**
     * Compiles a nested {@link ExpressionField} within an iterable field. Delegates to the source
     * strategy to produce a field reference qualified by the unnest alias.
     *
     * @param unnestAlias the alias of the UNNEST table (e.g., "item")
     * @param nestedField the nested expression field
     * @param strategy the source strategy for field access compilation
     * @param prefix the prefix for the absolute field name (e.g., "item.")
     */
    private static SelectField<?> compileNestedFieldExpression(
            String unnestAlias, ExpressionField nestedField, DuckDbSourceStrategy strategy, String prefix) {
        var nestedFieldName = nestedField.getFieldName();
        var absoluteFieldName = prefix + nestedFieldName;
        var reference = nestedField.getReference();

        if (reference != null) {
            return strategy.compileNestedFieldReference(unnestAlias, reference, fieldAlias(absoluteFieldName));
        }

        // Nested fields within UNNEST currently only support reference-based expressions
        throw new UnsupportedOperationException(
                "Nested field [%s] in iterable field [%s] must have a reference expression"
                        .formatted(nestedFieldName, unnestAlias));
    }

    /**
     * Compiles all {@link LogicalViewJoin}s from the view's left joins and inner joins into
     * {@link JoinDescriptor}s. Applies annotation-based optimizations:
     * <ul>
     *   <li>FK + not projected: eliminates the join entirely</li>
     *   <li>NotNull on child join keys: upgrades LEFT JOIN to INNER JOIN</li>
     * </ul>
     *
     * @param view the logical view containing joins
     * @param annotations the structural annotations on the view
     * @param notNullFieldNames field names covered by NotNull annotations
     * @param projectedFields the context's projected fields
     * @param strategy the source strategy for field access compilation
     * @return the list of compiled join descriptors (excluding eliminated joins)
     */
    private static List<JoinDescriptor> compileJoins(
            LogicalView view,
            Set<StructuralAnnotation> annotations,
            Set<String> notNullFieldNames,
            Set<String> projectedFields,
            DuckDbSourceStrategy strategy) {
        var joinDescriptors = new ArrayList<JoinDescriptor>();
        int parentIndex = 0;

        var leftJoins = view.getLeftJoins();
        if (leftJoins != null) {
            for (var viewJoin : leftJoins) {
                if (canEliminateJoin(viewJoin, annotations, projectedFields)) {
                    continue;
                }

                // Check if LEFT JOIN can be upgraded to INNER JOIN
                var effectivelyLeftJoin = !canUpgradeToInnerJoin(viewJoin, notNullFieldNames);
                joinDescriptors.add(compileJoinDescriptor(viewJoin, parentIndex++, effectivelyLeftJoin, strategy));
            }
        }

        var innerJoins = view.getInnerJoins();
        if (innerJoins != null) {
            for (var viewJoin : innerJoins) {
                if (canEliminateJoin(viewJoin, annotations, projectedFields)) {
                    continue;
                }

                joinDescriptors.add(compileJoinDescriptor(viewJoin, parentIndex++, false, strategy));
            }
        }

        return joinDescriptors;
    }

    /**
     * Compiles a single {@link LogicalViewJoin} into a {@link JoinDescriptor}.
     *
     * <p>The parent logical view is recursively compiled with default evaluation context, and the
     * resulting SQL is wrapped as a subquery table. The ON condition is built from the join's
     * conditions, and the projected fields from the parent are added to the SELECT.
     */
    private static JoinDescriptor compileJoinDescriptor(
            LogicalViewJoin viewJoin, int parentIndex, boolean isLeftJoin, DuckDbSourceStrategy strategy) {
        var parentView = viewJoin.getParentLogicalView();
        var parentAlias = PARENT_ALIAS_PREFIX + parentIndex;

        // Recursively compile the parent view
        var parentSql = compile(parentView, EvaluationContext.defaults());

        // Wrap as subquery table with alias
        var parentTable = table("({0})", sql(parentSql)).as(quotedName(parentAlias));

        // Build ON condition from join conditions
        var joinConditions = viewJoin.getJoinConditions();
        var onCondition = buildJoinCondition(joinConditions, parentAlias, strategy);

        // Build SELECT fields from the join's projected fields (including ordinal and type companions)
        var joinFields = new ArrayList<SelectField<?>>();
        for (var f : viewJoin.getFields()) {
            joinFields.add(compileJoinFieldExpression(parentAlias, f));
            // Project ordinal from parent view: "parent_N"."reference.#" AS "fieldName.#"
            if (f.getReference() != null) {
                joinFields.add(field(quotedName(parentAlias, f.getReference() + ".#"))
                        .as(quotedName(f.getFieldName() + ".#")));
                // Project type from parent view: "parent_N"."reference.__type" AS "fieldName.__type"
                joinFields.add(field(quotedName(parentAlias, f.getReference() + DuckDbSourceStrategy.TYPE_SUFFIX))
                        .as(quotedName(f.getFieldName() + DuckDbSourceStrategy.TYPE_SUFFIX)));
            }
        }

        return new JoinDescriptor(parentTable, onCondition, joinFields, isLeftJoin);
    }

    /**
     * Builds a compound ON condition from a set of {@link Join} conditions. Each join condition maps
     * a child reference to a parent reference. The child side is resolved via the source strategy.
     */
    private static Condition buildJoinCondition(
            Set<Join> joinConditions, String parentAlias, DuckDbSourceStrategy strategy) {
        return joinConditions.stream()
                .map(join -> {
                    var childRef = join.getChildMap().getReference();
                    var parentRef = join.getParentMap().getReference();
                    var childField = strategy.resolveJoinChildReference(childRef);
                    return childField.eq(field(quotedName(parentAlias, parentRef)));
                })
                .reduce(Condition::and)
                .orElseThrow(() -> new IllegalArgumentException("LogicalViewJoin has no join conditions"));
    }

    /**
     * Compiles a projected {@link ExpressionField} from a joined parent view, qualifying the
     * reference with the parent alias.
     */
    private static SelectField<?> compileJoinFieldExpression(String parentAlias, ExpressionField joinField) {
        var fieldName = joinField.getFieldName();
        var reference = joinField.getReference();

        if (reference != null) {
            return field(quotedName(parentAlias, reference)).as(fieldAlias(fieldName));
        }

        throw new UnsupportedOperationException(
                "Join projected field [%s] must have a reference expression".formatted(fieldName));
    }

    private static List<SelectField<?>> compileFieldSelects(
            List<ExpressionField> fields, DuckDbSourceStrategy strategy) {
        var selects = new ArrayList<SelectField<?>>();
        for (var f : fields) {
            selects.add(compileFieldExpression(f, strategy));
            // Add ordinal companion for reference-based expression fields.
            // Single-valued fields always have ordinal 0, cast to BIGINT to match range() type.
            if (f.getReference() != null) {
                selects.add(inline(0).cast(Long.class).as(quotedName(f.getFieldName() + ".#")));
                // Add type companion for reference-based expression fields.
                var typeAlias = quotedName(f.getFieldName() + DuckDbSourceStrategy.TYPE_SUFFIX);
                selects.add(strategy.compileFieldTypeReference(f.getReference(), typeAlias));
            }
        }
        return selects;
    }

    private static SelectField<?> compileFieldExpression(ExpressionField exprField, DuckDbSourceStrategy strategy) {
        var fieldName = exprField.getFieldName();

        var reference = exprField.getReference();
        if (reference != null) {
            return strategy.compileFieldReference(reference, fieldAlias(fieldName));
        }

        var template = exprField.getTemplate();
        if (template != null) {
            return compileTemplateExpression(template, fieldName, strategy);
        }

        var constant = exprField.getConstant();
        if (constant != null) {
            return inline(constant.stringValue()).as(fieldAlias(fieldName));
        }

        if (exprField.getFunctionValue() != null || exprField.getFunctionExecution() != null) {
            throw new UnsupportedOperationException(
                    "Function-based field expressions are not yet supported in DuckDB compilation: %s"
                            .formatted(fieldName));
        }

        throw new IllegalArgumentException(
                "ExpressionField [%s] has no reference, template, or constant defined".formatted(fieldName));
    }

    private static SelectField<?> compileTemplateExpression(
            Template template, String fieldName, DuckDbSourceStrategy strategy) {
        var segments = template.getSegments();
        var parts = segments.stream()
                .map(segment -> {
                    if (segment instanceof CarmlTemplate.ExpressionSegment) {
                        return strategy.compileTemplateReference(segment.getValue());
                    }
                    return inline(segment.getValue());
                })
                .toList();

        if (parts.size() == 1) {
            return parts.get(0).as(fieldAlias(fieldName));
        }

        // Build concatenation using DSL.concat
        var concatParts = parts.toArray(org.jooq.Field[]::new);
        return DSL.concat(concatParts).as(fieldAlias(fieldName));
    }

    private static org.jooq.Name fieldAlias(String alias) {
        if (alias.matches("[a-zA-Z_]\\w*")) {
            return name(alias);
        }
        return quotedName(alias);
    }
}
