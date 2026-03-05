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
import io.carml.model.DatabaseSource;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.IterableField;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.Source;
import io.carml.model.StructuralAnnotation;
import io.carml.model.Template;
import io.carml.model.UniqueAnnotation;
import io.carml.model.impl.CarmlTemplate;
import io.carml.vocab.Rdf;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
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

    private static final Set<String> PARQUET_EXTENSIONS = Set.of(".parquet", ".parq");

    private static final DSLContext CTX = DSL.using(SQLDialect.DUCKDB);

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
        var viewOn = view.getViewOn();

        if (viewOn instanceof LogicalView) {
            // TODO: Task 5.3+ — support view-on-view (nested CTE composition)
            throw new UnsupportedOperationException(
                    "View-on-view compilation is not yet supported. The viewOn target must be a LogicalSource.");
        }

        if (!(viewOn instanceof LogicalSource logicalSource)) {
            throw new IllegalArgumentException("Unsupported viewOn target type: %s"
                    .formatted(viewOn.getClass().getName()));
        }

        var sourceTable = compileSourceClause(logicalSource);
        var expressionFields = resolveExpressionFields(view.getFields(), context.getProjectedFields());
        var unnestDescriptors = resolveUnnestDescriptors(view.getFields(), context.getProjectedFields());

        // Extract structural annotations for optimization decisions
        var annotations = view.getStructuralAnnotations();
        var notNullFieldNames = extractNotNullFieldNames(annotations);

        var joinDescriptors = compileJoins(view, annotations, notNullFieldNames, context.getProjectedFields());

        // Determine whether DISTINCT is needed, considering annotation-based optimization
        var dedupRequested = !NONE_DEDUP_CLASS.isInstance(context.getDedupStrategy());
        var useDistinct = dedupRequested && !canSkipDistinct(annotations, notNullFieldNames, view, context);

        var viewSourceCte = name(CTE_ALIAS).as(CTX.select(asterisk()).from(sql(sourceTable)));

        String result;
        var allFieldSelects = collectAllFieldSelects(expressionFields, unnestDescriptors, joinDescriptors);

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
            allFieldSelects.add(rowNumber().over().as(name(INDEX_COLUMN)));

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
    private static ArrayList<SelectField<?>> collectAllFieldSelects(
            List<ExpressionField> expressionFields,
            List<UnnestDescriptor> unnestDescriptors,
            List<JoinDescriptor> joinDescriptors) {
        var allSelects = new ArrayList<>(compileFieldSelects(expressionFields));
        for (var unnest : unnestDescriptors) {
            allSelects.addAll(unnest.nestedSelects());
        }
        for (var joinDesc : joinDescriptors) {
            allSelects.addAll(joinDesc.fields());
        }
        return allSelects;
    }

    private static String compileSourceClause(LogicalSource logicalSource) {
        var refFormulation = logicalSource.getReferenceFormulation();
        if (refFormulation == null) {
            throw new IllegalArgumentException("LogicalSource has no reference formulation");
        }

        var refIri = refFormulation.getAsResource();
        return dispatchSourceFunction(refIri, logicalSource);
    }

    private static String dispatchSourceFunction(Resource refIri, LogicalSource logicalSource) {
        if (Rdf.Ql.JsonPath.equals(refIri) || Rdf.Rml.JsonPath.equals(refIri)) {
            return compileJsonSource(logicalSource);
        }
        if (Rdf.Ql.Csv.equals(refIri) || Rdf.Rml.Csv.equals(refIri)) {
            return compileCsvSource(logicalSource);
        }
        if (Rdf.Ql.Rdb.equals(refIri) || Rdf.Rml.SQL2008Table.equals(refIri) || Rdf.Rml.SQL2008Query.equals(refIri)) {
            return compileSqlSource(logicalSource);
        }
        if (Rdf.Ql.XPath.equals(refIri) || Rdf.Rml.XPath.equals(refIri)) {
            throw new UnsupportedOperationException(
                    "XPath/XML source compilation is not supported by the DuckDB evaluator. Use the reactive evaluator for XML sources.");
        }

        throw new IllegalArgumentException("Unknown reference formulation: %s".formatted(refIri));
    }

    private static String compileJsonSource(LogicalSource logicalSource) {
        var filePath = resolveFilePath(logicalSource.getSource());

        if (isParquetFile(filePath)) {
            return "read_parquet(%s)".formatted(inline(filePath));
        }

        var iterator = logicalSource.getIterator();
        if (iterator != null && !iterator.isBlank()) {
            return "read_json_auto(%s, json_path = %s)".formatted(inline(filePath), inline(iterator));
        }

        return "read_json_auto(%s)".formatted(inline(filePath));
    }

    private static String compileCsvSource(LogicalSource logicalSource) {
        var filePath = resolveFilePath(logicalSource.getSource());

        if (isParquetFile(filePath)) {
            return "read_parquet(%s)".formatted(inline(filePath));
        }

        return "read_csv_auto(%s)".formatted(inline(filePath));
    }

    private static String compileSqlSource(LogicalSource logicalSource) {
        var query = logicalSource.getQuery();
        if (query != null && !query.isBlank()) {
            return "(%s)".formatted(query);
        }

        var tableName = logicalSource.getTableName();
        if (tableName != null && !tableName.isBlank()) {
            return quotedName(tableName).toString();
        }

        var source = logicalSource.getSource();
        if (source instanceof DatabaseSource dbSource && dbSource.getQuery() != null) {
            return "(%s)".formatted(dbSource.getQuery());
        }

        throw new IllegalArgumentException("SQL logical source has no query or table name defined");
    }

    private static String resolveFilePath(Source source) {
        if (source instanceof FileSource fileSource) {
            var url = fileSource.getUrl();
            if (url == null || url.isBlank()) {
                throw new IllegalArgumentException("FileSource has no URL defined");
            }
            return url;
        }

        if (source instanceof FilePath filePath) {
            var path = filePath.getPath();
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("FilePath has no path defined");
            }
            return path;
        }

        if (source == null) {
            throw new IllegalArgumentException("LogicalSource has no source defined");
        }

        throw new IllegalArgumentException("Unsupported source type for file-based access: %s"
                .formatted(source.getClass().getName()));
    }

    private static boolean isParquetFile(String filePath) {
        var lowerPath = filePath.toLowerCase(Locale.ROOT);
        return PARQUET_EXTENSIONS.stream().anyMatch(lowerPath::endsWith);
    }

    /**
     * Resolves the top-level {@link ExpressionField} instances to include in the SELECT clause. If
     * projected fields is non-empty, only fields whose names match the projection are included.
     * Otherwise, all expression fields are included.
     */
    private static List<ExpressionField> resolveExpressionFields(Set<Field> viewFields, Set<String> projectedFields) {
        var expressionFields = viewFields.stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .toList();

        if (projectedFields.isEmpty()) {
            return expressionFields;
        }

        return expressionFields.stream()
                .filter(f -> projectedFields.contains(f.getFieldName()))
                .toList();
    }

    /**
     * Resolves {@link IterableField} instances from the view fields and produces
     * {@link UnnestDescriptor}s for UNNEST cross-joins. Each iterable field generates an UNNEST
     * table expression and nested SELECT fields qualified by the unnest alias.
     *
     * <p>If projected fields is non-empty, only nested fields whose names match the projection are
     * included.
     */
    private static List<UnnestDescriptor> resolveUnnestDescriptors(Set<Field> viewFields, Set<String> projectedFields) {
        return viewFields.stream()
                .filter(IterableField.class::isInstance)
                .map(IterableField.class::cast)
                .map(iterableField -> compileUnnestDescriptor(iterableField, projectedFields))
                .toList();
    }

    /**
     * Compiles a single {@link IterableField} into an {@link UnnestDescriptor} containing the
     * UNNEST table expression and the nested SELECT fields.
     */
    private static UnnestDescriptor compileUnnestDescriptor(IterableField iterableField, Set<String> projectedFields) {
        var iterableFieldName = iterableField.getFieldName();
        var iterator = iterableField.getIterator();

        if (iterator == null || iterator.isBlank()) {
            throw new IllegalArgumentException(
                    "IterableField [%s] has no iterator expression defined".formatted(iterableFieldName));
        }

        // Build: unnest("view_source"."iterator") AS "fieldName"
        var unnestTable =
                table("unnest({0})", field(quotedName(CTE_ALIAS, iterator))).as(quotedName(iterableFieldName));

        // Resolve nested expression fields
        var nestedFields = iterableField.getFields().stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .toList();

        var filteredNested = projectedFields.isEmpty()
                ? nestedFields
                : nestedFields.stream()
                        .filter(f -> projectedFields.contains(f.getFieldName()))
                        .toList();

        // Build SELECT fields qualified by the unnest alias: "fieldName"."reference" AS "nestedFieldName"
        var nestedSelects = filteredNested.stream()
                .<SelectField<?>>map(nested -> compileNestedFieldExpression(iterableFieldName, nested))
                .toList();

        return new UnnestDescriptor(unnestTable, nestedSelects);
    }

    /**
     * Compiles a nested {@link ExpressionField} within an iterable field, qualifying the reference
     * with the unnest alias.
     */
    private static SelectField<?> compileNestedFieldExpression(String unnestAlias, ExpressionField nestedField) {
        var nestedFieldName = nestedField.getFieldName();
        var reference = nestedField.getReference();

        if (reference != null) {
            return field(quotedName(unnestAlias, reference)).as(fieldAlias(nestedFieldName));
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
     * @return the list of compiled join descriptors (excluding eliminated joins)
     */
    private static List<JoinDescriptor> compileJoins(
            LogicalView view,
            Set<StructuralAnnotation> annotations,
            Set<String> notNullFieldNames,
            Set<String> projectedFields) {
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
                joinDescriptors.add(compileJoinDescriptor(viewJoin, parentIndex++, effectivelyLeftJoin));
            }
        }

        var innerJoins = view.getInnerJoins();
        if (innerJoins != null) {
            for (var viewJoin : innerJoins) {
                if (canEliminateJoin(viewJoin, annotations, projectedFields)) {
                    continue;
                }

                joinDescriptors.add(compileJoinDescriptor(viewJoin, parentIndex++, false));
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
    private static JoinDescriptor compileJoinDescriptor(LogicalViewJoin viewJoin, int parentIndex, boolean isLeftJoin) {
        var parentView = viewJoin.getParentLogicalView();
        var parentAlias = PARENT_ALIAS_PREFIX + parentIndex;

        // Recursively compile the parent view
        var parentSql = compile(parentView, EvaluationContext.defaults());

        // Wrap as subquery table with alias
        var parentTable = table("({0})", sql(parentSql)).as(quotedName(parentAlias));

        // Build ON condition from join conditions
        var joinConditions = viewJoin.getJoinConditions();
        var onCondition = buildJoinCondition(joinConditions, parentAlias);

        // Build SELECT fields from the join's projected fields
        var joinFields = viewJoin.getFields().stream()
                .<SelectField<?>>map(f -> compileJoinFieldExpression(parentAlias, f))
                .toList();

        return new JoinDescriptor(parentTable, onCondition, joinFields, isLeftJoin);
    }

    /**
     * Builds a compound ON condition from a set of {@link Join} conditions. Each join condition maps
     * a child reference to a parent reference.
     */
    private static Condition buildJoinCondition(Set<Join> joinConditions, String parentAlias) {
        return joinConditions.stream()
                .map(join -> {
                    var childRef = join.getChildMap().getReference();
                    var parentRef = join.getParentMap().getReference();
                    return field(quotedName(CTE_ALIAS, childRef)).eq(field(quotedName(parentAlias, parentRef)));
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

    private static List<SelectField<?>> compileFieldSelects(List<ExpressionField> fields) {
        return fields.stream()
                .<SelectField<?>>map(DuckDbViewCompiler::compileFieldExpression)
                .toList();
    }

    private static SelectField<?> compileFieldExpression(ExpressionField exprField) {
        var fieldName = exprField.getFieldName();

        var reference = exprField.getReference();
        if (reference != null) {
            return field(quotedName(reference)).as(fieldAlias(fieldName));
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
                    "Function-based field expressions are not yet supported in DuckDB compilation: %s"
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
                        return field(quotedName(segment.getValue()));
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
