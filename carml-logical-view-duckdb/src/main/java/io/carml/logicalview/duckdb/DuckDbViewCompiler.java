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
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.Source;
import io.carml.model.Template;
import io.carml.model.impl.CarmlTemplate;
import io.carml.vocab.Rdf;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.impl.DSL;

/**
 * Compiles a {@link LogicalView} and {@link EvaluationContext} into a DuckDB SQL query string.
 *
 * <p>This is a pure function class with no side effects and no DuckDB connection. It produces
 * CTE-structured SQL using WITH clauses for view composition.
 *
 * <p>Uses jOOQ's type-safe DSL for SQL construction with {@link SQLDialect#DUCKDB}.
 *
 * <p>Currently supports basic field compilation (expression fields mapped to SELECT columns).
 * Iterable fields (UNNEST), joins, and structural annotation optimizations are handled in later
 * tasks.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DuckDbViewCompiler {

    static final String INDEX_COLUMN = "__idx";

    private static final String CTE_ALIAS = "view_source";

    private static final String DEDUPED_ALIAS = "deduped";

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
     * Compiles a {@link LogicalView} into a DuckDB SQL query string.
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
        var fields = resolveFields(view.getFields(), context.getProjectedFields());
        var useDistinct = !NONE_DEDUP_CLASS.isInstance(context.getDedupStrategy());

        var viewSourceCte = name(CTE_ALIAS).as(CTX.select(asterisk()).from(sql(sourceTable)));

        String result;
        if (useDistinct) {
            var fieldSelects = compileFieldSelects(fields);
            var dedupedCte = name(DEDUPED_ALIAS)
                    .as(CTX.selectDistinct(fieldSelects.toArray(SelectField[]::new))
                            .from(table(name(CTE_ALIAS))));

            var outerQuery = CTX.with(viewSourceCte)
                    .with(dedupedCte)
                    .select(asterisk(), rowNumber().over().as(name(INDEX_COLUMN)))
                    .from(table(name(DEDUPED_ALIAS)));

            result = context.getLimit()
                    .map(limit -> outerQuery.limit(limit).getSQL())
                    .orElseGet(outerQuery::getSQL);
        } else {
            var selectFields = new ArrayList<>(compileFieldSelects(fields));
            selectFields.add(rowNumber().over().as(name(INDEX_COLUMN)));

            var query = CTX.with(viewSourceCte).select(selectFields).from(table(name(CTE_ALIAS)));

            result =
                    context.getLimit().map(limit -> query.limit(limit).getSQL()).orElseGet(query::getSQL);
        }

        LOG.debug("Compiled DuckDB SQL for view [{}]:\n{}", view.getResourceName(), result);
        return result;
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
     * Resolves the set of fields to include in the SELECT clause. If projected fields is non-empty,
     * only fields whose names match the projection are included. Otherwise, all fields are included.
     */
    private static List<ExpressionField> resolveFields(Set<Field> viewFields, Set<String> projectedFields) {
        var expressionFields = viewFields.stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .toList();

        // Log a warning for any IterableField instances (handled in Task 5.3)
        viewFields.stream()
                .filter(IterableField.class::isInstance)
                .forEach(field -> LOG.warn(
                        "IterableField [{}] is not yet supported in DuckDB compilation and will be skipped",
                        field.getFieldName()));

        if (projectedFields.isEmpty()) {
            return expressionFields;
        }

        return expressionFields.stream()
                .filter(field -> projectedFields.contains(field.getFieldName()))
                .toList();
    }

    private static List<SelectField<?>> compileFieldSelects(List<ExpressionField> fields) {
        return fields.stream().<SelectField<?>>map(DuckDbViewCompiler::compileFieldExpression).toList();
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
