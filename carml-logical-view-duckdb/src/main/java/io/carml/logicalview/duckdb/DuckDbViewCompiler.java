package io.carml.logicalview.duckdb;

import static io.carml.logicalview.duckdb.DuckDbSourceStrategy.ORDINAL_FIELD;
import static io.carml.logicalview.duckdb.DuckDbSourceStrategy.TYPE_SUFFIX;
import static io.carml.util.LogUtil.exception;
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
import io.carml.model.ExpressionMap;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.IterableField;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.Mapping;
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
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.QueryPart;
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
 * <p>Source-specific compilation details (JSON field extraction, UNNEST expansion, multi-valued
 * field handling) are delegated to {@link DuckDbSourceStrategy} implementations. The compiler
 * orchestrates the overall SQL structure while the strategies handle formulation-specific logic.
 *
 * <p><b>IriSafe annotation note:</b> {@code IriSafeAnnotation} does not currently affect SQL
 * compilation. Templates already use raw {@code CONCAT} without percent-encoding. When IRI encoding
 * support is added to the DuckDB evaluator (e.g., via a UDF), IriSafe annotations should be used
 * to skip that encoding step for annotated fields.
 */
@SuppressWarnings("UnstableApiUsage")
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DuckDbViewCompiler {

    static final String INDEX_COLUMN = "__idx";

    private static final String SUBQUERY_TEMPLATE = "({0})";

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
     * Resolves source SQL expressions to cached source table names. When set, the compiler replaces
     * file-reading source SQL (e.g., {@code read_json_auto('file.json')}) with the cached table name,
     * ensuring the file is read only once even when multiple views share the same source.
     *
     * <p>Set by the outermost {@link #compile(LogicalView, EvaluationContext, UnaryOperator)} call and
     * inherited by recursive calls for parent views.
     */
    private static final ThreadLocal<UnaryOperator<String>> SOURCE_TABLE_RESOLVER = new ThreadLocal<>();

    /**
     * The database attacher for the current compilation. When set, SQL source handlers use it to
     * ATTACH remote databases and produce fully qualified table references, eliminating the need for
     * {@code USE} on duplicated connections.
     *
     * <p>Set by the outermost {@link #compile(LogicalView, EvaluationContext, UnaryOperator,
     * DuckDbDatabaseAttacher)} call and inherited by recursive calls for parent views.
     */
    private static final ThreadLocal<DuckDbDatabaseAttacher> DATABASE_ATTACHER = new ThreadLocal<>();

    /**
     * The NDJSON transcode cache for the current compilation. When set, the JSON source handler
     * substitutes the default {@code read_text} + {@code json_extract} clause with a stream-friendly
     * {@code read_ndjson_objects} clause for large JSON-array files.
     *
     * <p>Set by the outermost {@link #compile(LogicalView, EvaluationContext, UnaryOperator,
     * DuckDbDatabaseAttacher, JsonNdjsonTranscodeCache)} call and inherited by recursive calls.
     */
    private static final ThreadLocal<JsonNdjsonTranscodeCache> NDJSON_CACHE = new ThreadLocal<>();

    /**
     * The active {@link Mapping} for the current compilation. Source handlers read
     * it via {@link #currentMapping()} when they need to apply {@code rml:root} anchor semantics
     * (e.g. {@code rml:MappingDirectory}). Bound by the outermost {@link #compile} call so that
     * recursive parent-view compilations see the same mapping context.
     */
    private static final ThreadLocal<Mapping> MAPPING = new ThreadLocal<>();

    /**
     * Cached class reference for the none dedup strategy, used to detect whether deduplication
     * should be applied. The concrete class is package-private in {@code io.carml.logicalview},
     * so we resolve it once via the public factory method.
     */
    private static final Class<? extends DedupStrategy> NONE_DEDUP_CLASS =
            DedupStrategy.none().getClass();

    /**
     * Describes one logical-view join in compiled form. {@code inline=true} marks descriptors whose
     * {@code fields} are computed as window-function projections on {@code view_source} directly,
     * with no JOIN clause emitted ({@code table} and {@code condition} are unused). Used for
     * aggregating self-joins to avoid HASH_JOIN materialization of list-typed columns.
     *
     * @param table the jOOQ subquery table for the parent view; {@code null} when {@code inline=true}
     * @param condition the ON condition for the join; {@code null} when {@code inline=true}
     * @param fields the SELECT fields projected from the parent (or window-function expressions
     *     when {@code inline=true})
     * @param isLeftJoin {@code true} for LEFT JOIN, {@code false} for INNER JOIN; ignored when
     *     {@code inline=true}
     * @param inline {@code true} to emit the {@code fields} as inline window-function expressions
     *     over {@code view_source}, with no JOIN clause
     */
    private record JoinDescriptor(
            Table<?> table, Condition condition, List<SelectField<?>> fields, boolean isLeftJoin, boolean inline) {

        static JoinDescriptor join(
                Table<?> table, Condition condition, List<SelectField<?>> fields, boolean isLeftJoin) {
            return new JoinDescriptor(table, condition, fields, isLeftJoin, false);
        }

        static JoinDescriptor inline(List<SelectField<?>> fields) {
            return new JoinDescriptor(null, null, fields, false, true);
        }
    }

    /**
     * Compiles a {@link LogicalView} with a source table resolver that caches source SQL as temp
     * tables. The resolver is set as a ThreadLocal so that recursive parent view compilations also
     * benefit from the cache.
     *
     * @param view the logical view defining fields and the underlying data source
     * @param context the evaluation context controlling projection, dedup, and limits
     * @param sourceTableResolver resolves source SQL to a cached source table name, or {@code null}
     *     to use source SQL directly
     * @return the compiled view containing the SQL query and validation metadata
     */
    static CompiledView compile(
            LogicalView view, EvaluationContext context, UnaryOperator<String> sourceTableResolver) {
        return compile(view, context, sourceTableResolver, null);
    }

    /**
     * Compiles a {@link LogicalView} with a source table resolver and a database attacher. Both are
     * set as ThreadLocals so that recursive parent view compilations inherit them.
     *
     * @param view the logical view defining fields and the underlying data source
     * @param context the evaluation context controlling projection, dedup, and limits
     * @param sourceTableResolver resolves source SQL to a cached source table name, or {@code null}
     *     to use source SQL directly
     * @param databaseAttacher the attacher for SQL database sources, or {@code null} if not available
     * @return the compiled view containing the SQL query and validation metadata
     */
    static CompiledView compile(
            LogicalView view,
            EvaluationContext context,
            UnaryOperator<String> sourceTableResolver,
            DuckDbDatabaseAttacher databaseAttacher) {
        return compile(view, context, sourceTableResolver, databaseAttacher, null);
    }

    /**
     * Compiles a {@link LogicalView} with a source table resolver, database attacher, and NDJSON
     * transcode cache. All three are set as ThreadLocals so recursive parent view compilations
     * inherit them. Each ThreadLocal is cleared by the outermost caller in a {@code try/finally}
     * block.
     *
     * @param view the logical view defining fields and the underlying data source
     * @param context the evaluation context controlling projection, dedup, and limits
     * @param sourceTableResolver resolves source SQL to a cached source table name, or {@code null}
     *     to use source SQL directly
     * @param databaseAttacher the attacher for SQL database sources, or {@code null} if not available
     * @param ndjsonTranscodeCache the cache used by the JSON source handler to stream-transcode
     *     large JSON-array files into NDJSON, or {@code null} when transcoding is not enabled
     * @return the compiled view containing the SQL query and validation metadata
     */
    static CompiledView compile(
            LogicalView view,
            EvaluationContext context,
            UnaryOperator<String> sourceTableResolver,
            DuckDbDatabaseAttacher databaseAttacher,
            JsonNdjsonTranscodeCache ndjsonTranscodeCache) {
        return compile(view, context, sourceTableResolver, databaseAttacher, ndjsonTranscodeCache, null);
    }

    /**
     * Compiles a {@link LogicalView} with a source table resolver, database attacher, NDJSON
     * transcode cache, and {@link io.carml.model.Mapping} context. All four are set as
     * ThreadLocals so recursive parent view compilations inherit them. Each ThreadLocal is cleared
     * by the outermost caller in a {@code try/finally} block.
     *
     * @param view the logical view defining fields and the underlying data source
     * @param context the evaluation context controlling projection, dedup, and limits
     * @param sourceTableResolver resolves source SQL to a cached source table name, or {@code null}
     *     to use source SQL directly
     * @param databaseAttacher the attacher for SQL database sources, or {@code null} if not available
     * @param ndjsonTranscodeCache the cache used by the JSON source handler to stream-transcode
     *     large JSON-array files into NDJSON, or {@code null} when transcoding is not enabled
     * @param mapping the active mapping, used by source handlers to apply {@code rml:root}
     *     anchor semantics; {@code null} when no mapping context is available
     * @return the compiled view containing the SQL query and validation metadata
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    static CompiledView compile(
            LogicalView view,
            EvaluationContext context,
            UnaryOperator<String> sourceTableResolver,
            DuckDbDatabaseAttacher databaseAttacher,
            JsonNdjsonTranscodeCache ndjsonTranscodeCache,
            Mapping mapping) {
        var isOutermostResolverCall = sourceTableResolver != null && SOURCE_TABLE_RESOLVER.get() == null;
        var isOutermostAttacherCall = databaseAttacher != null && DATABASE_ATTACHER.get() == null;
        var isOutermostNdjsonCall = ndjsonTranscodeCache != null && NDJSON_CACHE.get() == null;
        var isOutermostMappingCall = mapping != null && MAPPING.get() == null;

        if (isOutermostResolverCall) {
            SOURCE_TABLE_RESOLVER.set(sourceTableResolver);
        }
        if (isOutermostAttacherCall) {
            DATABASE_ATTACHER.set(databaseAttacher);
        }
        if (isOutermostNdjsonCall) {
            NDJSON_CACHE.set(ndjsonTranscodeCache);
        }
        if (isOutermostMappingCall) {
            MAPPING.set(mapping);
        }
        try {
            return compile(view, context);
        } finally {
            if (isOutermostResolverCall) {
                SOURCE_TABLE_RESOLVER.remove();
            }
            if (isOutermostAttacherCall) {
                DATABASE_ATTACHER.remove();
            }
            if (isOutermostNdjsonCall) {
                NDJSON_CACHE.remove();
            }
            if (isOutermostMappingCall) {
                MAPPING.remove();
            }
        }
    }

    /**
     * Compiles a {@link LogicalView} into a {@link CompiledView} containing the DuckDB SQL query
     * string and metadata needed for post-query validation.
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
     * @return the compiled view containing the SQL query and validation metadata
     * @throws IllegalArgumentException if the view's source type is unsupported or the view
     *     structure cannot be compiled
     */
    static CompiledView compile(LogicalView view, EvaluationContext context) {
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

    private static CompiledView doCompile(LogicalView view, EvaluationContext context) {
        var viewOn = view.getViewOn();

        DuckDbSourceStrategy strategy;
        String sourceTable;

        if (viewOn instanceof LogicalView innerView) {
            // Recursively compile the inner view and wrap its SQL as a subquery.
            // The inner view produces a complete CTE query (WITH ... SELECT ...),
            // so it must be parenthesized to be used as a derived table in FROM.
            // ColumnSourceStrategy is used because the inner view exposes named columns.
            var innerCompiledView = compile(innerView, EvaluationContext.defaults());
            sourceTable = "(%s)".formatted(innerCompiledView.sql());
            strategy = ColumnSourceStrategy.create(
                    view.getFields(), CTE_ALIAS, ColumnSourceStrategy.TypeCompanionMode.INNER_VIEW);
        } else if (viewOn instanceof LogicalSource logicalSource) {
            var compiledSource = compileSourceClause(logicalSource, view);
            strategy = compiledSource.strategy();
            var resolver = SOURCE_TABLE_RESOLVER.get();
            if (resolver != null) {
                var tableName = resolver.apply(compiledSource.sourceSql());
                if (tableName != null) {
                    // The resolver returns a fully qualified table reference (e.g.,
                    // "memory"."main"."__carml_src_abc12345_0") from DuckDbSourceTableCache,
                    // so it can be used as-is regardless of the connection's current catalog.
                    sourceTable = tableName;
                } else {
                    sourceTable = compiledSource.sourceSql();
                }
            } else {
                sourceTable = compiledSource.sourceSql();
            }
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

        var joinDescriptors = compileJoins(
                view,
                annotations,
                notNullFieldNames,
                context.getProjectedFields(),
                context.getAggregatingJoins(),
                strategy);

        // Determine whether DISTINCT is needed, considering annotation-based optimization
        var dedupRequested = !NONE_DEDUP_CLASS.isInstance(context.getDedupStrategy());
        var useDistinct = dedupRequested && !canSkipDistinct(annotations, notNullFieldNames, view, context);

        // ROW_NUMBER() is computed inside the CTE so that each source row gets a stable index
        // that is preserved through UNNEST expansion and JOIN multiplication.
        var viewSourceCte = name(CTE_ALIAS)
                .as(CTX.select(asterisk(), rowNumber().over().as(name(INDEX_COLUMN)))
                        .from(sql(sourceTable)));

        String sql;
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

            sql = context.getLimit()
                    .map(limit -> outerQuery.limit(limit).getSQL())
                    .orElseGet(outerQuery::getSQL);
        } else {
            // Reference the pre-computed index from the CTE
            allFieldSelects.add(field(quotedName(CTE_ALIAS, INDEX_COLUMN)));

            // Include raw source evaluation column for source-level expression evaluation.
            // For JSON iterator sources, the __iter column carries the raw JSON for each iteration
            // row, enabling DuckDbJsonSourceEvaluation to evaluate expressions using JSONPath at
            // mapping time. This serves two purposes:
            //   1. Gather map expressions (excluded from view fields) need raw source evaluation
            //   2. Expressions that cannot be compiled to SQL (e.g., function-based fields) fall
            //      back to per-row JSONPath evaluation via the engine's withSourceFallback mechanism
            // The __iter column is always included when the strategy supports it, not just for
            // implicit views. It is only added in the non-DISTINCT path because the raw data blob
            // would interfere with deduplication.
            strategy.sourceEvaluationColumn()
                    .ifPresent(column -> allFieldSelects.add(
                            field(quotedName(CTE_ALIAS, column)).as(quotedName(column))));

            var fromStep = buildFromClause(
                    CTX.with(viewSourceCte).select(allFieldSelects), unnestDescriptors, joinDescriptors);

            sql = context.getLimit()
                    .map(limit -> fromStep.limit(limit).getSQL())
                    .orElseGet(fromStep::getSQL);
        }

        LOG.debug("Compiled DuckDB SQL for view [{}]:\n{}", view.getResourceName(), sql);

        // Collect metadata for the evaluator: multi-valued field names and non-scalar type values.
        // This is computed here during compilation because the strategy (which knows about
        // multi-valued references and non-scalar types) is already resolved.
        var multiValuedFieldNames = view.getFields().stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .filter(f -> isMultiValuedExpressionField(f, strategy))
                .map(Field::getFieldName)
                .collect(Collectors.toUnmodifiableSet());

        return new CompiledView(sql, multiValuedFieldNames, strategy.nonScalarTypeValues());
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
     * Applies JOIN clauses (left or inner) to a select-join step. Inline descriptors are skipped:
     * their projections are emitted as window-function expressions over {@code view_source}, with
     * no JOIN clause needed.
     */
    private static <T extends org.jooq.Record> org.jooq.SelectJoinStep<T> applyJoins(
            org.jooq.SelectJoinStep<T> fromStep, List<JoinDescriptor> joinDescriptors) {
        var current = fromStep;
        for (var joinDesc : joinDescriptors) {
            if (joinDesc.inline()) {
                continue;
            }
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
     * Returns the NDJSON transcode cache active on the current compilation thread, or {@code null}
     * when no cache is bound. Read by {@link JsonPathSourceHandler} so that the JSON-specific cache
     * does not need to leak through the {@link DuckDbSourceHandler} SPI. The returned reference is
     * borrowed from the owning factory; callers must not close it.
     */
    @SuppressWarnings("resource")
    static JsonNdjsonTranscodeCache currentNdjsonTranscodeCache() {
        return NDJSON_CACHE.get();
    }

    /**
     * Returns the database attacher active on the current compilation thread, or {@code null} when
     * no attacher is bound. Read by {@link SqlSourceHandler} so that the SQL-specific attacher does
     * not need to leak through the {@link DuckDbSourceHandler} SPI.
     */
    static DuckDbDatabaseAttacher currentDatabaseAttacher() {
        return DATABASE_ATTACHER.get();
    }

    /**
     * Returns the {@link Mapping} active on the current compilation thread, or
     * {@code null} when no mapping context is bound. Read by file-source handlers so they can
     * apply {@code rml:root} anchor semantics (notably {@code rml:MappingDirectory}) without the
     * mapping reference leaking through the {@link DuckDbSourceHandler} SPI.
     */
    static Mapping currentMapping() {
        return MAPPING.get();
    }

    /**
     * Binds {@code mapping} as the active compilation mapping for the duration of {@code action},
     * restoring the previous binding (or removing the thread-local) on exit. Used by callers that
     * need source-handler context to honour {@code rml:root} anchors outside the compile path —
     * e.g. pre-compile validators that go through {@link DuckDbSourceHandler#validate} and read
     * {@link #currentMapping()} via {@link DuckDbFileSourceUtils#resolveFilePath}.
     *
     * <p>Re-entrant calls preserve the outer binding: only the outermost call clears the
     * thread-local on exit, so nesting compile or validate steps inside {@code action} see the
     * mapping as expected.
     */
    static void withMapping(Mapping mapping, Runnable action) {
        if (mapping == null || MAPPING.get() != null) {
            action.run();
            return;
        }
        MAPPING.set(mapping);
        try {
            action.run();
        } finally {
            MAPPING.remove();
        }
    }

    /**
     * Resolves the top-level {@link ExpressionField} instances to include in the SELECT clause,
     * excluding multi-valued fields which are handled as UNNESTs. If projected fields is non-empty,
     * only fields whose names match the projection are included.
     *
     * <p>An expression field is included when the projection contains its field name directly, or
     * its ordinal key ({@code fieldName.#}), or any nested field path ({@code fieldName.child.*}).
     * This ensures expression fields with mixed-formulation children are selected when the mapping
     * references their ordinal or nested fields from child iterables.
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
                .filter(f -> isExpressionFieldProjected(f, projectedFields))
                .toList();
    }

    /**
     * Checks whether an expression field is needed by the projection. An expression field is
     * projected when:
     * <ul>
     *   <li>The projection contains the field name directly (e.g., "items")</li>
     *   <li>The projection contains the ordinal key (e.g., "items.#")</li>
     *   <li>The projection contains any nested field path (e.g., "items.item.type"), indicating
     *       a mixed-formulation child iterable needs the parent expression field's ordinal</li>
     * </ul>
     */
    private static boolean isExpressionFieldProjected(ExpressionField field, Set<String> projectedFields) {
        var fieldName = field.getFieldName();
        if (projectedFields.contains(fieldName)) {
            return true;
        }
        // Check for ordinal or nested field references (fieldName.*)
        var prefix = fieldName + ".";
        return projectedFields.stream().anyMatch(p -> p.startsWith(prefix));
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
                .map(f -> strategy.compileMultiValuedUnnestDescriptor(f, CTE_ALIAS))
                .toList();
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
            } else if (field instanceof ExpressionField expressionField) {
                // Check for child IterableFields with a different reference formulation (mixed-formulation).
                // E.g., a CSV column containing JSON text, or a JSON field containing CSV text.
                var childFields = expressionField.getFields();
                if (childFields != null && !childFields.isEmpty()) {
                    collectMixedFormulationUnnestDescriptors(
                            expressionField, childFields, projectedFields, strategy, prefix, result);
                }
            }
        }
    }

    // --- Mixed-formulation iterable field support ---

    /**
     * Checks child fields of an {@link ExpressionField} for nested {@link IterableField}s that use
     * a different reference formulation than the parent source (mixed-formulation). For each such
     * child, compiles an appropriate UNNEST descriptor.
     *
     * <p>This handles cases like:
     * <ul>
     *   <li>CSV parent with a column containing JSON text (parsed via JSONPath iterable)</li>
     *   <li>JSON parent with a field containing CSV text (parsed via CSV iterable)</li>
     * </ul>
     *
     * @param parentField the parent expression field whose value contains the embedded data
     * @param childFields the child fields of the parent expression field
     * @param projectedFields the context's projected fields
     * @param strategy the parent source strategy
     * @param prefix the dot-separated prefix for absolute field names
     * @param result the accumulator for descriptors
     */
    private static void collectMixedFormulationUnnestDescriptors(
            ExpressionField parentField,
            Set<Field> childFields,
            Set<String> projectedFields,
            DuckDbSourceStrategy strategy,
            String prefix,
            List<UnnestDescriptor> result) {
        var parentValueField = strategy.compileTemplateReference(parentField.getReference());

        for (var child : childFields) {
            if (child instanceof IterableField iterableChild && iterableChild.getReferenceFormulation() != null) {
                var childFormulationIri =
                        iterableChild.getReferenceFormulation().getAsResource();
                var absoluteName = prefix + parentField.getFieldName() + "." + iterableChild.getFieldName();

                var compiler = MixedFormulationCompiler.forFormulation(childFormulationIri, parentValueField);
                if (compiler.isPresent()) {
                    result.add(compileMixedFormulationUnnestDescriptor(
                            iterableChild, projectedFields, absoluteName, compiler.get()));
                } else {
                    LOG.warn(
                            "Unsupported mixed-formulation child reference formulation [{}] in field [{}]",
                            childFormulationIri,
                            absoluteName);
                }
            }
        }
    }

    /**
     * Compiles a mixed-formulation UNNEST descriptor using the given {@link MixedFormulationCompiler}.
     * The compiler encapsulates formulation-specific logic for UNNEST table generation, nested field
     * extraction, and type companion generation. Adding support for a new reference formulation
     * requires only a new {@link MixedFormulationCompiler} implementation.
     */
    private static UnnestDescriptor compileMixedFormulationUnnestDescriptor(
            IterableField iterableChild,
            Set<String> projectedFields,
            String absoluteName,
            MixedFormulationCompiler compiler) {
        var unnestTable = compiler.compileUnnestTable(iterableChild.getIterator(), absoluteName);
        var filteredNested = filterNestedExpressionFields(iterableChild, projectedFields, absoluteName);

        var nestedPrefix = absoluteName + ".";
        var nestedSelects = new ArrayList<SelectField<?>>();
        for (var nested : filteredNested) {
            var absoluteFieldName = nestedPrefix + nested.getFieldName();
            var reference = nested.getReference();
            if (reference != null) {
                nestedSelects.add(compiler.compileNestedField(absoluteName, reference, fieldAlias(absoluteFieldName)));
                var typeAlias = quotedName(absoluteFieldName + TYPE_SUFFIX);
                nestedSelects.add(compiler.compileNestedFieldType(absoluteName, reference, typeAlias));
            }
        }

        appendOrdinalColumn(nestedSelects, absoluteName);
        return new UnnestDescriptor(unnestTable, nestedSelects);
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
        var filteredNested = filterNestedExpressionFields(iterableField, projectedFields, absoluteName);

        var nestedPrefix = absoluteName + ".";
        var nestedSelects = new ArrayList<SelectField<?>>();
        for (var nested : filteredNested) {
            nestedSelects.add(compileNestedFieldExpression(absoluteName, nested, strategy, nestedPrefix));
            if (nested.getReference() != null) {
                var absoluteFieldName = nestedPrefix + nested.getFieldName();
                var typeAlias = quotedName(absoluteFieldName + DuckDbSourceStrategy.TYPE_SUFFIX);
                nestedSelects.add(
                        strategy.compileNestedFieldTypeReference(absoluteName, nested.getReference(), typeAlias));
            }
        }

        appendOrdinalColumn(nestedSelects, absoluteName);
        return new UnnestDescriptor(unnestTable, nestedSelects);
    }

    /**
     * Filters nested {@link ExpressionField}s from an iterable field's children, applying projection
     * filtering when projected fields are specified.
     */
    private static List<ExpressionField> filterNestedExpressionFields(
            IterableField iterableField, Set<String> projectedFields, String absoluteName) {
        var nestedFields = iterableField.getFields().stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .toList();

        if (projectedFields.isEmpty()) {
            return nestedFields;
        }

        var nestedPrefix = absoluteName + ".";
        return nestedFields.stream()
                .filter(expressionField -> projectedFields.contains(nestedPrefix + expressionField.getFieldName()))
                .toList();
    }

    /**
     * Appends the ordinal column ({@code absoluteName + ".#"}) to the nested selects list.
     */
    private static void appendOrdinalColumn(List<SelectField<?>> nestedSelects, String absoluteName) {
        var indexColumnName = absoluteName + ".#";
        nestedSelects.add(field(quotedName(absoluteName, ORDINAL_FIELD)).as(quotedName(indexColumnName)));
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
            Set<LogicalViewJoin> aggregatingJoins,
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
                joinDescriptors.add(compileJoinDescriptor(
                        viewJoin,
                        view,
                        parentIndex++,
                        effectivelyLeftJoin,
                        aggregatingJoins.contains(viewJoin),
                        strategy));
            }
        }

        var innerJoins = view.getInnerJoins();
        if (innerJoins != null) {
            for (var viewJoin : innerJoins) {
                if (canEliminateJoin(viewJoin, annotations, projectedFields)) {
                    continue;
                }

                joinDescriptors.add(compileJoinDescriptor(
                        viewJoin, view, parentIndex++, false, aggregatingJoins.contains(viewJoin), strategy));
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
     *
     * <p>For aggregating joins (used by {@code rml:gather} with joined RefObjectMaps), the parent
     * view is wrapped in a GROUP BY subquery that aggregates all parent values into DuckDB list
     * columns, so a single child row gets a list of all matching parent values instead of row
     * multiplication.
     */
    private static JoinDescriptor compileJoinDescriptor(
            LogicalViewJoin viewJoin,
            LogicalView childView,
            int parentIndex,
            boolean isLeftJoin,
            boolean isAggregating,
            DuckDbSourceStrategy strategy) {
        var parentView = viewJoin.getParentLogicalView();
        var parentAlias = PARENT_ALIAS_PREFIX + parentIndex;

        if (isAggregating && isSelfJoinSameColumn(viewJoin, childView)) {
            // Self-join + same-column join condition: rewrite to inline window-function projections
            // on view_source. Avoids the HASH_JOIN that would materialize a list-typed column per
            // matched row — that materialization OOMs DuckDB at high group cardinality even with
            // spill enabled (the projection isn't shared across matched rows).
            return compileSelfJoinAggregatingViaWindow(viewJoin);
        }

        // Recursively compile the parent view (for non-self-join aggregating cases and all
        // non-aggregating joins, the parent SQL is needed as a subquery table).
        var parentCompiledView = compile(parentView, EvaluationContext.defaults());
        var parentSql = parentCompiledView.sql();

        if (isAggregating) {
            return compileAggregatingJoinDescriptor(viewJoin, parentSql, parentAlias, isLeftJoin, strategy);
        }

        // Wrap as subquery table with alias
        var parentTable = table(SUBQUERY_TEMPLATE, sql(parentSql)).as(quotedName(parentAlias));

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

        return JoinDescriptor.join(parentTable, onCondition, joinFields, isLeftJoin);
    }

    /**
     * Returns true when {@code viewJoin} is a self-join whose conditions all bind the same column on
     * both sides (e.g. {@code child.Sport = parent.Sport}) AND the child view's FROM clause is
     * row-stable (no UNNEST cross-joins). For these joins the aggregating projection collapses into
     * a window function over {@code view_source} — the parent is structurally identical to the
     * child, so {@code list(col ORDER BY __idx) OVER (PARTITION BY …)} yields the same per-group
     * lists that the equivalent {@code SELECT … GROUP BY … LEFT JOIN} would produce, without DuckDB
     * materializing the list-typed projection per matched row.
     *
     * <p>The row-stability check excludes views with {@link IterableField} children: UNNEST would
     * multiply {@code view_source} rows post-FROM, and the window's {@code PARTITION BY join_col}
     * would aggregate over UNNEST-multiplied rows rather than over the source rows the equivalent
     * GROUP-BY subquery sees pre-UNNEST. The aggregating-LEFT-JOIN path handles those cases
     * correctly because it aggregates the parent subquery before any child-side UNNEST applies.
     */
    private static boolean isSelfJoinSameColumn(LogicalViewJoin viewJoin, LogicalView childView) {
        if (childView == null) {
            return false;
        }
        var parentSource = viewJoin.getParentLogicalView() != null
                ? viewJoin.getParentLogicalView().getViewOn()
                : null;
        if (parentSource == null || !parentSource.equals(childView.getViewOn())) {
            return false;
        }
        var conditions = viewJoin.getJoinConditions();
        if (conditions == null || conditions.isEmpty()) {
            return false;
        }
        if (hasUnnestField(childView.getFields())) {
            return false;
        }
        return conditions.stream().allMatch(join -> {
            var childRef = join.getChildMap().getReference();
            var parentRef = join.getParentMap().getReference();
            return childRef != null && childRef.equals(parentRef);
        });
    }

    private static boolean hasUnnestField(java.util.Collection<? extends Field> fields) {
        if (fields == null) {
            return false;
        }
        return fields.stream().anyMatch(DuckDbViewCompiler::fieldHasUnnest);
    }

    private static boolean fieldHasUnnest(Field field) {
        if (field instanceof IterableField) {
            return true;
        }
        return hasUnnestField(field.getFields());
    }

    /**
     * Builds an inline {@link JoinDescriptor} that emits the join's projected fields as window
     * aggregates over {@code view_source}. Equivalent to the aggregating {@code GROUP BY} subquery
     * + {@code LEFT JOIN} but without DuckDB's HASH_JOIN duplicating the list-typed projection
     * across matched rows.
     */
    private static JoinDescriptor compileSelfJoinAggregatingViaWindow(LogicalViewJoin viewJoin) {
        var partitionRefs = viewJoin.getJoinConditions().stream()
                .map(join -> join.getChildMap().getReference())
                .filter(Objects::nonNull)
                .sorted()
                .distinct()
                .toList();

        if (partitionRefs.isEmpty()) {
            throw new IllegalStateException(
                    "Self-join aggregating join %s has no partition columns".formatted(exception(viewJoin)));
        }

        var orderByField = field(quotedName(CTE_ALIAS, INDEX_COLUMN));
        var partitionFields = partitionRefs.stream()
                .map(ref -> (QueryPart) field(quotedName(CTE_ALIAS, ref)))
                .toList();

        // Build placeholder positions {2}, {3}, ... for partition fields, prefixed by {0} and {1}
        // for the projected column and the ORDER BY column respectively. jOOQ's
        // DSL.field(String, QueryPart...) substitutes positional placeholders safely.
        var partitionPlaceholder = IntStream.range(0, partitionFields.size())
                .mapToObj(i -> "{" + (i + 2) + "}")
                .collect(Collectors.joining(", "));
        var windowSql = "list({0} order by {1}) over (partition by " + partitionPlaceholder + ")";

        var windowFields = new ArrayList<SelectField<?>>();
        for (var f : viewJoin.getFields()) {
            if (f.getReference() == null) {
                continue;
            }
            var col = field(quotedName(CTE_ALIAS, f.getReference()));
            var args = new ArrayList<QueryPart>();
            args.add(col);
            args.add(orderByField);
            args.addAll(partitionFields);
            var windowExpr = DSL.field(windowSql, args.toArray(new QueryPart[0]));
            windowFields.add(windowExpr.as(quotedName(f.getFieldName())));
        }

        return JoinDescriptor.inline(windowFields);
    }

    /**
     * Compiles an aggregating join descriptor. Wraps the parent SQL in a GROUP BY subquery that
     * uses DuckDB's {@code list()} aggregate to collect all values into list columns per group key.
     */
    private static JoinDescriptor compileAggregatingJoinDescriptor(
            LogicalViewJoin viewJoin,
            String parentSql,
            String parentAlias,
            boolean isLeftJoin,
            DuckDbSourceStrategy strategy) {

        // Determine GROUP BY fields from join condition parent references
        var groupByRefs = viewJoin.getJoinConditions().stream()
                .map(join -> join.getParentMap().getReference())
                .sorted()
                .distinct()
                .toList();

        // Determine projected (aggregated) fields from join data fields
        var projectedRefs = viewJoin.getFields().stream()
                .map(io.carml.model.ExpressionField::getReference)
                .filter(Objects::nonNull)
                .sorted()
                .distinct()
                .toList();

        // Build the aggregating subquery:
        // SELECT "groupByRef", list("projRef" ORDER BY "__idx") AS "projRef", ...
        // FROM (<parentSql>) __agg GROUP BY "groupByRef"
        // ORDER BY __idx preserves source row order and ensures all projected fields are
        // sorted consistently (maintaining tuple correspondence across columns).
        var aggAlias = "__agg";
        var orderByField = field(quotedName(aggAlias, INDEX_COLUMN));
        var aggSelects = new ArrayList<SelectField<?>>();
        for (var groupByRef : groupByRefs) {
            aggSelects.add(field(quotedName(aggAlias, groupByRef)));
        }
        for (var projRef : projectedRefs) {
            var projField = field(quotedName(aggAlias, projRef));
            aggSelects.add(
                    DSL.field("list({0} order by {1})", projField, orderByField).as(quotedName(projRef)));
        }

        var groupByFields = groupByRefs.stream()
                .map(ref -> field(quotedName(aggAlias, ref)))
                .toArray(org.jooq.Field[]::new);

        var aggQuery = CTX.select(aggSelects)
                .from(table(SUBQUERY_TEMPLATE, sql(parentSql)).as(quotedName(aggAlias)))
                .groupBy(groupByFields);

        var parentTable = table(SUBQUERY_TEMPLATE, sql(aggQuery.getSQL())).as(quotedName(parentAlias));

        // Build ON condition
        var onCondition = buildJoinCondition(viewJoin.getJoinConditions(), parentAlias, strategy);

        // Build SELECT fields — aggregated fields are already lists, no ordinal/type companions
        var joinFields = new ArrayList<SelectField<?>>();
        for (var f : viewJoin.getFields()) {
            joinFields.add(compileJoinFieldExpression(parentAlias, f));
        }

        return JoinDescriptor.join(parentTable, onCondition, joinFields, isLeftJoin);
    }

    /**
     * Builds a compound ON condition from a set of {@link Join} conditions. Each join condition has
     * an {@link io.carml.model.ChildMap} and an {@link io.carml.model.ParentMap}, both of which are
     * full {@link ExpressionMap}s supporting reference, template, and constant shapes. The child
     * side is resolved via the source strategy; the parent side resolves directly against the
     * parent view's materialized columns (each parent reference is exposed as a column with that
     * name, see {@code ImplicitViewFactory#wrap}).
     */
    private static Condition buildJoinCondition(
            Set<Join> joinConditions, String parentAlias, DuckDbSourceStrategy strategy) {
        return joinConditions.stream()
                .map(join -> {
                    var childField = strategy.resolveJoinChildExpression(join.getChildMap());
                    var parentField = compileJoinExpressionMap(
                            join.getParentMap(),
                            reference -> field(quotedName(parentAlias, reference)),
                            templateVariable -> field(quotedName(parentAlias, templateVariable)));
                    return joinKeyEq(childField, parentField);
                })
                .reduce(Condition::and)
                .orElseThrow(() -> new IllegalArgumentException("LogicalViewJoin has no join conditions"));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Condition joinKeyEq(org.jooq.Field<?> left, org.jooq.Field<?> right) {
        return ((org.jooq.Field) left).eq(right);
    }

    /**
     * Compiles a join-path {@link ExpressionMap} into a jOOQ {@link org.jooq.Field}. Used for both
     * sides of a {@link io.carml.model.Join} condition (child / parent maps) and for parent-view
     * projection fields exposed through a {@link LogicalViewJoin}. Supports the three RML 2
     * expression-map shapes:
     *
     * <ul>
     *   <li>reference — resolved via {@code referenceResolver}
     *   <li>template — concatenation of literal segments and template-variable lookups via
     *       {@code templateVariableResolver}
     *   <li>constant — a SQL string literal produced via {@link DSL#inline(Object)}
     * </ul>
     *
     * <p>The following shapes are not supported on the join path and result in an
     * {@link UnsupportedOperationException}, allowing the engine to fall back to the reactive
     * evaluator:
     *
     * <ul>
     *   <li>function-valued expression maps ({@code rml:functionValue} or
     *       {@code rml:functionExecution})
     *   <li>expression maps carrying {@code rml:condition} — gating conditions are not yet
     *       compilable to SQL on the join path
     *   <li>empty expression maps (none of reference, template, constant set)
     * </ul>
     *
     * @param exprMap the expression map to compile
     * @param referenceResolver maps a reference string to its compiled field
     * @param templateVariableResolver maps a template variable string to its compiled field
     */
    @SuppressWarnings("java:S1452")
    static org.jooq.Field<?> compileJoinExpressionMap(
            ExpressionMap exprMap,
            Function<String, org.jooq.Field<?>> referenceResolver,
            Function<String, org.jooq.Field<?>> templateVariableResolver) {
        if (exprMap.getFunctionValue() != null || exprMap.getFunctionExecution() != null) {
            throw new UnsupportedOperationException(
                    ("Join expression map cannot be compiled to SQL: function-valued expression maps are not supported"
                                    + " on the join path: %s")
                            .formatted(exception(exprMap)));
        }

        if (!exprMap.getConditions().isEmpty()) {
            throw new UnsupportedOperationException(
                    ("Join expression map cannot be compiled to SQL: rml:condition is not yet supported on join"
                                    + " expression maps: %s")
                            .formatted(exception(exprMap)));
        }

        var reference = exprMap.getReference();
        if (reference != null) {
            return referenceResolver.apply(reference);
        }

        var template = exprMap.getTemplate();
        if (template != null) {
            var parts = template.getSegments().stream()
                    .map(segment -> segment instanceof CarmlTemplate.ExpressionSegment
                            ? templateVariableResolver.apply(segment.getValue())
                            : inline(segment.getValue()))
                    .toList();
            if (parts.size() == 1) {
                return parts.get(0);
            }
            return DSL.concat(parts.toArray(org.jooq.Field[]::new));
        }

        var constant = exprMap.getConstant();
        if (constant != null) {
            return inline(constant.stringValue());
        }

        throw new UnsupportedOperationException(
                ("Join expression map cannot be compiled to SQL: no reference, template, constant, or function"
                                + " expression is set: %s")
                        .formatted(exception(exprMap)));
    }

    /**
     * Compiles a projected {@link ExpressionField} from a joined parent view. Reuses
     * {@link #compileJoinExpressionMap} with parent-alias-qualified column resolvers, so the
     * projection field supports the same reference / template / constant shapes as the join
     * condition. The compiled jOOQ field is wrapped with the projection field's alias.
     *
     * <p>Template variables resolve to columns of the compiled parent subquery — every parent view
     * field is materialized as a column named after its {@code fieldName}, so a template variable
     * naming a parent field becomes a direct column reference qualified with {@code parentAlias}.
     */
    private static SelectField<?> compileJoinFieldExpression(String parentAlias, ExpressionField joinField) {
        var compiled = compileJoinExpressionMap(
                joinField,
                reference -> field(quotedName(parentAlias, reference)),
                templateVariable -> field(quotedName(parentAlias, templateVariable)));
        return compiled.as(fieldAlias(joinField.getFieldName()));
    }

    private static List<SelectField<?>> compileFieldSelects(
            List<ExpressionField> fields, DuckDbSourceStrategy strategy) {
        var selects = new ArrayList<SelectField<?>>();
        var hasSourceFallback = strategy.sourceEvaluationColumn().isPresent();
        for (var field : fields) {
            SelectField<?> compiled;
            try {
                compiled = compileFieldExpression(field, strategy);
            } catch (UnsupportedOperationException e) {
                if (hasSourceFallback) {
                    // Skip this field — it will be evaluated per-row from the raw source data
                    // via the engine's withSourceFallback mechanism. This is slower than SQL-compiled
                    // evaluation but ensures correctness for expressions the SQL compiler cannot handle.
                    LOG.warn(
                            "Field [{}] cannot be compiled to SQL and will use per-row source evaluation fallback"
                                    + " (slower). Reason: {}",
                            field.getFieldName(),
                            e.getMessage());
                    continue;
                }
                throw e;
            }
            selects.add(compiled);
            // Add ordinal companion for reference-based expression fields.
            // Single-valued fields always have ordinal 0, cast to BIGINT to match range() type.
            if (field.getReference() != null) {
                selects.add(inline(0).cast(Long.class).as(quotedName(field.getFieldName() + ".#")));
                // Add type companion for reference-based expression fields.
                var typeAlias = quotedName(field.getFieldName() + DuckDbSourceStrategy.TYPE_SUFFIX);
                selects.add(strategy.compileFieldTypeReference(field.getReference(), typeAlias));
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

    static org.jooq.Name fieldAlias(String alias) {
        if (alias.matches("[a-zA-Z_]\\w*")) {
            return name(alias);
        }
        return quotedName(alias);
    }
}
