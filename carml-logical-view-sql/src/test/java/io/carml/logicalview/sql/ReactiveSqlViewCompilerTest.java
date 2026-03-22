package io.carml.logicalview.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.logicalview.DedupStrategy;
import io.carml.logicalview.EvaluationContext;
import io.carml.model.AbstractLogicalSource;
import io.carml.model.ChildMap;
import io.carml.model.DatabaseSource;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.ParentMap;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.StructuralAnnotation;
import io.carml.model.Template;
import io.carml.model.UniqueAnnotation;
import io.carml.model.impl.CarmlTemplate;
import java.util.List;
import java.util.Set;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ReactiveSqlViewCompilerTest {

    // --- Table source tests ---

    @Nested
    class TableSource {

        @Test
        void compile_tableSourceWithExpressionFields_producesCteWithRowNumberAndProjection() {
            var view = createSqlViewWithTable(
                    "users", Set.of(expressionField("id", "id"), expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("with \"view_source\" as ("));
            assertThat(sql, containsString("row_number() over ()"));
            assertThat(sql, containsString("as \"__idx\""));
            assertThat(sql, containsString("from \"view_source\""));
        }

        @Test
        void compile_tableSourceWithSingleField_producesFieldProjection() {
            var view = createSqlViewWithTable("employees", Set.of(expressionField("emp_name", "name")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            // jOOQ renders: "view_source"."name" as "emp_name"
            assertThat(sql, containsString("\"view_source\".\"name\" as \"emp_name\""));
        }

        @Test
        void compile_tableSource_quotesTableName() {
            var view = createSqlViewWithTable("my_table", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            // quotedName().toString() always uses double quotes
            assertThat(sql, containsString("\"my_table\""));
        }
    }

    // --- Query source tests ---

    @Nested
    class QuerySource {

        @Test
        void compile_querySource_usesQueryAsSubquery() {
            var view = createSqlViewWithQuery("SELECT id, name FROM users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("(SELECT id, name FROM users)"));
        }

        @Test
        void compile_querySourceWithTrailingSemicolon_stripsSemicolon() {
            var view = createSqlViewWithQuery("SELECT * FROM orders;  ", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("(SELECT * FROM orders)"));
            assertThat(sql, not(containsString(";")));
        }

        @Test
        void compile_databaseSourceFallbackQuery_usesQueryAsSubquery() {
            var dbSource = mock(DatabaseSource.class);
            when(dbSource.getQuery()).thenReturn("SELECT * FROM orders");

            var view = createSqlViewWithDatabaseSource(dbSource, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("(SELECT * FROM orders)"));
        }

        @Test
        void compile_noQueryNoTableNoDbSourceQuery_throwsIllegalArgumentException() {
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getQuery()).thenReturn(null);
            when(logicalSource.getTableName()).thenReturn(null);
            when(logicalSource.getSource()).thenReturn(null);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);
            lenient().when(view.getResourceName()).thenReturn("testView");

            var context = EvaluationContext.defaults();

            assertThrows(
                    IllegalArgumentException.class,
                    () -> ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES));
        }
    }

    // --- Template field tests ---

    @Nested
    class TemplateFieldTests {

        @Test
        void compile_templateFieldWithMultipleSegments_producesConcatOrPipe() {
            var segment1 = new CarmlTemplate.TextSegment("http://example.org/");
            var segment2 = new CarmlTemplate.ExpressionSegment(0, "id");
            var template = mock(Template.class);
            lenient().when(template.getSegments()).thenReturn(List.of(segment1, segment2));

            var field = mock(ExpressionField.class);
            lenient().when(field.getFieldName()).thenReturn("iri");
            lenient().when(field.getReference()).thenReturn(null);
            lenient().when(field.getTemplate()).thenReturn(template);

            var view = createSqlViewWithTable("users", Set.of(field, expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            // PostgreSQL uses || for string concatenation
            assertThat(sql, containsString("'http://example.org/'"));
            assertThat(sql, containsString("\"view_source\".\"id\""));
            assertThat(sql, containsString("||"));
            assertThat(sql, containsString("as \"iri\""));
        }

        @Test
        void compile_singleSegmentTemplate_omitsConcatenation() {
            var segment = new CarmlTemplate.ExpressionSegment(0, "id");
            var template = mock(Template.class);
            lenient().when(template.getSegments()).thenReturn(List.of(segment));

            var field = mock(ExpressionField.class);
            lenient().when(field.getFieldName()).thenReturn("iri");
            lenient().when(field.getReference()).thenReturn(null);
            lenient().when(field.getTemplate()).thenReturn(template);

            var view = createSqlViewWithTable("users", Set.of(field));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            // Single segment should not use concat or || — just the column reference
            assertThat(sql, not(containsString("||")));
            assertThat(sql, containsString("\"view_source\".\"id\""));
        }

        @Test
        void compile_mysqlTemplateFieldWithMultipleSegments_producesConcat() {
            var segment1 = new CarmlTemplate.TextSegment("http://example.org/");
            var segment2 = new CarmlTemplate.ExpressionSegment(0, "id");
            var template = mock(Template.class);
            lenient().when(template.getSegments()).thenReturn(List.of(segment1, segment2));

            var field = mock(ExpressionField.class);
            lenient().when(field.getFieldName()).thenReturn("iri");
            lenient().when(field.getReference()).thenReturn(null);
            lenient().when(field.getTemplate()).thenReturn(template);

            var view = createSqlViewWithTable("users", Set.of(field));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.MYSQL);

            // MySQL uses concat() function
            assertThat(sql, containsString("concat("));
        }
    }

    // --- Constant field tests ---

    @Nested
    class ConstantField {

        @Test
        void compile_constantField_producesInlineConstant() {
            var vf = SimpleValueFactory.getInstance();
            var field = mock(ExpressionField.class);
            lenient().when(field.getFieldName()).thenReturn("type");
            lenient().when(field.getReference()).thenReturn(null);
            lenient().when(field.getTemplate()).thenReturn(null);
            lenient().when(field.getConstant()).thenReturn(vf.createLiteral("Person"));

            var view = createSqlViewWithTable("users", Set.of(field));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("'Person'"));
            assertThat(sql, containsString("as \"type\""));
        }
    }

    // --- Limit tests ---

    @Nested
    class Limiting {

        @Test
        void compile_withLimit_appendsLimitClause() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            // Use MYSQL dialect which renders LIMIT N directly (not FETCH NEXT ? ROWS ONLY)
            var context = EvaluationContext.withProjectedFieldsAndLimit(Set.of(), 100L);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.MYSQL);

            assertThat(sql, containsString("limit"));
        }

        @Test
        void compile_withLimitPostgres_appendsFetchNextClause() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.withProjectedFieldsAndLimit(Set.of(), 100L);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            // PostgreSQL jOOQ renders LIMIT as "fetch next ? rows only" with bind variable
            assertThat(sql, containsString("fetch next"));
            assertThat(sql, containsString("rows only"));
        }

        @Test
        void compile_withoutLimit_omitsLimitClause() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, not(containsString("limit")));
            assertThat(sql, not(containsString("fetch next")));
        }
    }

    // --- Deduplication tests ---

    @Nested
    class Deduplication {

        @Test
        void compile_withNoneDedupStrategy_omitsDistinct() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, not(containsString("select distinct")));
            assertThat(sql, not(containsString("\"deduped\"")));
        }

        @Test
        void compile_withExactDedupStrategy_usesDistinctCte() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("\"deduped\" as ("));
            assertThat(sql, containsString("select distinct"));
            assertThat(sql, containsString("from \"deduped\""));
            // ROW_NUMBER assigned after dedup in the outer SELECT
            assertThat(sql, containsString("select *, row_number() over ()"));
        }

        @Test
        void compile_withDistinctAndLimit_includesBoth() {
            var view = createSqlViewWithTable(
                    "users", Set.of(expressionField("name", "name"), expressionField("age", "age")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), 50L);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.MYSQL);

            assertThat(sql, containsString("select distinct"));
            assertThat(sql, containsString("limit"));
            assertThat(sql, containsString("`deduped`"));
        }

        @Test
        void compile_withSimpleEqualityDedupStrategy_usesDistinctCte() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.simpleEquality(), null);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("\"deduped\" as ("));
            assertThat(sql, containsString("select distinct"));
        }
    }

    // --- Column projection tests ---

    @Nested
    class ColumnProjection {

        @Test
        void compile_withProjectedFields_selectsOnlyProjectedFields() {
            var view = createSqlViewWithTable(
                    "users",
                    Set.of(
                            expressionField("name", "name"),
                            expressionField("age", "age"),
                            expressionField("email", "email")));
            var context = EvaluationContext.withProjectedFields(Set.of("name"));

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("as \"name\""));
            assertThat(sql, not(containsString("as \"age\"")));
            assertThat(sql, not(containsString("as \"email\"")));
        }

        @Test
        void compile_withEmptyProjectedFields_selectsAllFields() {
            var view = createSqlViewWithTable(
                    "users", Set.of(expressionField("name", "name"), expressionField("age", "age")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("as \"name\""));
            assertThat(sql, containsString("as \"age\""));
        }
    }

    // --- Dialect tests ---

    @Nested
    class DialectVariations {

        @Test
        void compile_mysqlDialect_usesBackticksForJooqIdentifiers() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.MYSQL);

            // MySQL uses backticks for jOOQ-managed identifiers
            assertThat(sql, containsString("`view_source`"));
            assertThat(sql, containsString("`__idx`"));
        }

        @Test
        void compile_postgresDialect_usesDoubleQuotes() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            // PostgreSQL uses double quotes
            assertThat(sql, containsString("\"view_source\""));
            assertThat(sql, containsString("\"__idx\""));
        }

        @Test
        void compile_mysqlDialect_producesLimitSyntax() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.withProjectedFieldsAndLimit(Set.of(), 10L);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.MYSQL);

            assertThat(sql, containsString("limit"));
        }

        @Test
        void compile_postgresDialect_producesFetchNextSyntax() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.withProjectedFieldsAndLimit(Set.of(), 10L);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("fetch next"));
        }
    }

    // --- SQL structure verification ---

    @Nested
    class SqlStructure {

        @Test
        void compile_simpleTable_producesWellFormedCte() {
            var view = createSqlViewWithTable("people", Set.of(expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("with \"view_source\" as ("));
            assertThat(sql, containsString("select *"));
            assertThat(sql, containsString("row_number() over ()"));
            assertThat(sql, containsString("as \"__idx\""));
            assertThat(sql, containsString("from \"people\""));
            assertThat(sql, containsString("from \"view_source\""));
        }

        @Test
        void compile_withDistinct_producesCorrectDedupCteStructure() {
            var view = createSqlViewWithTable("people", Set.of(expressionField("name", "name")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            // Should have two CTEs: view_source and deduped
            assertThat(sql, containsString("\"view_source\" as ("));
            assertThat(sql, containsString("\"deduped\" as ("));
            assertThat(sql, containsString("select distinct"));
            // Outer query re-numbers with ROW_NUMBER
            assertThat(sql, containsString("select *, row_number() over ()"));
            assertThat(sql, containsString("from \"deduped\""));
        }

        @Test
        void compile_withoutDistinct_includesIdxFromCte() {
            var view = createSqlViewWithTable("people", Set.of(expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            // Without dedup, the __idx column should be selected from view_source directly
            assertThat(sql, containsString("\"view_source\".\"__idx\""));
        }
    }

    // --- Index column tests ---

    @Nested
    class IndexColumn {

        @Test
        void compile_withoutDedup_projectsIdxFromViewSource() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("\"view_source\".\"__idx\""));
        }

        @Test
        void compile_withDedup_reNumbersRowsInOuterQuery() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("select *, row_number() over ()"));
            assertThat(sql, containsString("from \"deduped\""));
        }
    }

    // --- Join tests ---

    @Nested
    class JoinCompilation {

        @Test
        void compile_leftJoinWithSingleCondition_producesLeftJoin() {
            var parentView = createSqlViewWithTable(
                    "departments", Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var view = createSqlViewWithTableAndJoins(
                    "employees", Set.of(expressionField("emp_name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("left outer join"));
            assertThat(sql, containsString("\"parent_0\""));
            assertThat(sql, containsString("\"view_source\".\"dept_id\" = \"parent_0\".\"dept_id\""));
            // Join projected field uses "as" keyword for alias
            assertThat(sql, containsString("\"parent_0\".\"dept_name\" as \"department_name\""));
        }

        @Test
        void compile_innerJoinWithSingleCondition_producesInnerJoin() {
            var parentView = createSqlViewWithTable(
                    "departments", Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var view = createSqlViewWithTableAndJoins(
                    "employees", Set.of(expressionField("emp_name", "name")), Set.of(), Set.of(viewJoin));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("join ("));
            assertThat(sql, not(containsString("left outer join")));
        }

        @Test
        void compile_multipleJoinConditions_producesCompoundOnClause() {
            var parentView = createSqlViewWithTable(
                    "details", Set.of(expressionField("id", "id"), expressionField("region", "region")));

            var joinField = expressionField("detail_region", "region");
            var cond1 = joinCondition("id", "id");
            var cond2 = joinCondition("region", "region");
            var viewJoin = logicalViewJoin(parentView, Set.of(cond1, cond2), Set.of(joinField));

            var view = createSqlViewWithTableAndJoins(
                    "main", Set.of(expressionField("name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, containsString("\"view_source\".\"id\" = \"parent_0\".\"id\""));
            assertThat(sql, containsString("\"view_source\".\"region\" = \"parent_0\".\"region\""));
            assertThat(sql, containsString("and"));
        }

        @Test
        void compile_joinWithNoConditions_throwsIllegalArgumentException() {
            var parentView = createSqlViewWithTable("departments", Set.of(expressionField("dept_id", "id")));

            var viewJoin = logicalViewJoin(parentView, Set.of(), Set.of(expressionField("dept_name", "name")));
            var view = createSqlViewWithTableAndJoins(
                    "employees", Set.of(expressionField("name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            assertThrows(
                    IllegalArgumentException.class,
                    () -> ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES));
        }

        @Test
        void compile_joinFieldWithNoReference_throwsUnsupportedOperationException() {
            var parentView = createSqlViewWithTable("departments", Set.of(expressionField("dept_id", "id")));

            var brokenField = mock(ExpressionField.class);
            when(brokenField.getFieldName()).thenReturn("dept_name");
            when(brokenField.getReference()).thenReturn(null);

            var viewJoin =
                    logicalViewJoin(parentView, Set.of(joinCondition("dept_id", "dept_id")), Set.of(brokenField));
            var view = createSqlViewWithTableAndJoins(
                    "employees", Set.of(expressionField("name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            assertThrows(
                    UnsupportedOperationException.class,
                    () -> ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES));
        }
    }

    // --- View-on-view composition tests ---

    @Nested
    class ViewOnView {

        @Test
        void compile_viewOnView_compilesInnerViewAsSubquery() {
            var innerView = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var outerView = createViewOnView(innerView, Set.of(expressionField("user_id", "id")));

            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(outerView, context, SQLDialect.POSTGRES);

            // The inner view SQL should appear nested inside the outer CTE
            assertThat(sql, containsString("with \"view_source\" as ("));
            assertThat(sql, containsString("\"users\""));
        }

        @Test
        void compile_cyclicViewOnView_throwsIllegalArgumentException() {
            var view = mock(LogicalView.class);
            lenient().when(view.getResourceName()).thenReturn("cyclicView");
            lenient().when(view.getViewOn()).thenReturn(view);
            lenient().when(view.getFields()).thenReturn(Set.of());
            lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

            var context = EvaluationContext.defaults();

            assertThrows(
                    IllegalArgumentException.class,
                    () -> ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES));
        }
    }

    // --- Unsupported source type tests ---

    @Nested
    class UnsupportedSourceTypes {

        @Test
        void compile_unsupportedViewOnType_throwsIllegalArgumentException() {
            var unsupportedSource = mock(AbstractLogicalSource.class);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(unsupportedSource);
            lenient().when(view.getResourceName()).thenReturn("testView");
            lenient().when(view.getFields()).thenReturn(Set.of());
            lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

            var context = EvaluationContext.defaults();

            assertThrows(
                    IllegalArgumentException.class,
                    () -> ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES));
        }

        @Test
        void compile_fieldWithFunctionExecution_throwsUnsupportedOperationException() {
            var field = mock(ExpressionField.class);
            lenient().when(field.getFieldName()).thenReturn("computed");
            lenient().when(field.getReference()).thenReturn(null);
            lenient().when(field.getTemplate()).thenReturn(null);
            lenient().when(field.getConstant()).thenReturn(null);
            lenient().when(field.getFunctionExecution()).thenReturn(mock(io.carml.model.FunctionExecution.class));

            var view = createSqlViewWithTable("users", Set.of(field));
            var context = EvaluationContext.defaults();

            assertThrows(
                    UnsupportedOperationException.class,
                    () -> ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES));
        }

        @Test
        void compile_fieldWithNoExpressionAtAll_throwsIllegalArgumentException() {
            var field = mock(ExpressionField.class);
            lenient().when(field.getFieldName()).thenReturn("empty");
            lenient().when(field.getReference()).thenReturn(null);
            lenient().when(field.getTemplate()).thenReturn(null);
            lenient().when(field.getConstant()).thenReturn(null);

            var view = createSqlViewWithTable("users", Set.of(field));
            var context = EvaluationContext.defaults();

            assertThrows(
                    IllegalArgumentException.class,
                    () -> ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES));
        }
    }

    // --- Structural annotation optimization tests ---

    @Nested
    class AnnotationOptimizations {

        @Test
        void compile_primaryKeyCoveringSelectedFields_omitsDistinct() {
            var idField = expressionField("id", "id");
            var nameField = expressionField("name", "name");
            var pkAnnotation = primaryKeyAnnotation(idField);

            var view = createSqlViewWithTableAndAnnotations("users", Set.of(idField, nameField), Set.of(pkAnnotation));
            var context = EvaluationContext.of(Set.of("id", "name"), DedupStrategy.exact(), null);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, not(containsString("select distinct")));
            assertThat(sql, not(containsString("\"deduped\"")));
        }

        @Test
        void compile_uniqueAndNotNullCoveringSelectedFields_omitsDistinct() {
            var idField = expressionField("id", "id");
            var nameField = expressionField("name", "name");
            var uniqueAnno = uniqueAnnotation(idField);
            var notNullAnno = notNullAnnotation(idField);

            var view = createSqlViewWithTableAndAnnotations(
                    "users", Set.of(idField, nameField), Set.of(uniqueAnno, notNullAnno));
            var context = EvaluationContext.of(Set.of("id", "name"), DedupStrategy.exact(), null);

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, not(containsString("select distinct")));
        }

        @Test
        void compile_foreignKeyWithNoProjectedParentFields_eliminatesJoin() {
            var parentView = createSqlViewWithTable("departments", Set.of(expressionField("dept_id", "id")));

            var joinField = expressionField("dept_name", "name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var deptIdField = expressionField("dept_id", "dept_id");
            var fkAnnotation =
                    foreignKeyAnnotation(parentView, List.of(deptIdField), List.of(expressionField("dept_id", "id")));

            var view = createSqlViewWithTableJoinsAndAnnotations(
                    "employees",
                    Set.of(expressionField("emp_name", "name"), deptIdField),
                    Set.of(viewJoin),
                    Set.of(),
                    Set.of(fkAnnotation));
            // Project only emp_name (not dept_name from join), so join can be eliminated
            var context = EvaluationContext.withProjectedFields(Set.of("emp_name"));

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            assertThat(sql, not(containsString("left outer join")));
            assertThat(sql, not(containsString("\"parent_0\"")));
        }

        @Test
        void compile_notNullOnChildJoinKeys_upgradesLeftToInnerJoin() {
            var parentView = createSqlViewWithTable(
                    "departments", Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var deptIdField = expressionField("dept_id", "dept_id");
            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var notNull = notNullAnnotation(deptIdField);

            var view = createSqlViewWithTableJoinsAndAnnotations(
                    "employees",
                    Set.of(expressionField("emp_name", "name"), deptIdField),
                    Set.of(viewJoin),
                    Set.of(),
                    Set.of(notNull));
            var context = EvaluationContext.defaults();

            var sql = ReactiveSqlViewCompiler.compile(view, context, SQLDialect.POSTGRES);

            // NotNull on dept_id (child join key) should upgrade LEFT JOIN to INNER JOIN
            assertThat(sql, not(containsString("left outer join")));
            assertThat(sql, containsString("join ("));
        }
    }

    // --- Thread safety tests ---

    @Nested
    class ThreadSafety {

        @Test
        void compile_afterException_cleansUpThreadLocal() {
            var view = mock(LogicalView.class);
            lenient().when(view.getViewOn()).thenReturn(mock(AbstractLogicalSource.class));
            lenient().when(view.getFields()).thenReturn(Set.of());
            lenient().when(view.getLeftJoins()).thenReturn(Set.of());
            lenient().when(view.getInnerJoins()).thenReturn(Set.of());
            lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());
            lenient().when(view.getResourceName()).thenReturn("failView");

            assertThrows(
                    IllegalArgumentException.class,
                    () -> ReactiveSqlViewCompiler.compile(view, EvaluationContext.defaults(), SQLDialect.POSTGRES));

            // After the exception the ThreadLocal must be clean
            var goodView = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var sql = ReactiveSqlViewCompiler.compile(goodView, EvaluationContext.defaults(), SQLDialect.POSTGRES);
            assertThat(sql, containsString("\"users\""));
        }
    }

    // --- Helper methods ---

    private static ExpressionField expressionField(String fieldName, String reference) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(reference);
        return field;
    }

    private static Join joinCondition(String childRef, String parentRef) {
        var childMap = mock(ChildMap.class);
        lenient().when(childMap.getReference()).thenReturn(childRef);

        var parentMap = mock(ParentMap.class);
        lenient().when(parentMap.getReference()).thenReturn(parentRef);

        var join = mock(Join.class);
        lenient().when(join.getChildMap()).thenReturn(childMap);
        lenient().when(join.getParentMap()).thenReturn(parentMap);

        return join;
    }

    private static LogicalViewJoin logicalViewJoin(
            LogicalView parentView, Set<Join> joinConditions, Set<ExpressionField> fields) {
        var viewJoin = mock(LogicalViewJoin.class);
        lenient().when(viewJoin.getParentLogicalView()).thenReturn(parentView);
        lenient().when(viewJoin.getJoinConditions()).thenReturn(joinConditions);
        lenient().when(viewJoin.getFields()).thenReturn(fields);
        return viewJoin;
    }

    private static PrimaryKeyAnnotation primaryKeyAnnotation(ExpressionField... fields) {
        var annotation = mock(PrimaryKeyAnnotation.class);
        lenient().when(annotation.getOnFields()).thenReturn(List.of(fields));
        return annotation;
    }

    private static UniqueAnnotation uniqueAnnotation(ExpressionField... fields) {
        var annotation = mock(UniqueAnnotation.class);
        lenient().when(annotation.getOnFields()).thenReturn(List.of(fields));
        return annotation;
    }

    private static NotNullAnnotation notNullAnnotation(ExpressionField... fields) {
        var annotation = mock(NotNullAnnotation.class);
        lenient().when(annotation.getOnFields()).thenReturn(List.of(fields));
        return annotation;
    }

    private static ForeignKeyAnnotation foreignKeyAnnotation(
            LogicalView targetView, List<ExpressionField> onFields, List<ExpressionField> targetFields) {
        var annotation = mock(ForeignKeyAnnotation.class);
        lenient().when(annotation.getOnFields()).thenReturn(List.copyOf(onFields));
        lenient().when(annotation.getTargetView()).thenReturn(targetView);
        lenient().when(annotation.getTargetFields()).thenReturn(List.copyOf(targetFields));
        return annotation;
    }

    @SuppressWarnings("unchecked")
    private static LogicalView createSqlViewWithTable(String tableName, Set<ExpressionField> fields) {
        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getQuery()).thenReturn(null);
        lenient().when(logicalSource.getTableName()).thenReturn(tableName);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        var fieldSet = (Set<Field>) (Set<?>) fields;
        lenient().when(view.getFields()).thenReturn(fieldSet);
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());
        lenient().when(view.getLeftJoins()).thenReturn(Set.of());
        lenient().when(view.getInnerJoins()).thenReturn(Set.of());

        return view;
    }

    @SuppressWarnings("unchecked")
    private static LogicalView createSqlViewWithQuery(String query, Set<ExpressionField> fields) {
        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getQuery()).thenReturn(query);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        var fieldSet = (Set<Field>) (Set<?>) fields;
        lenient().when(view.getFields()).thenReturn(fieldSet);
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());
        lenient().when(view.getLeftJoins()).thenReturn(Set.of());
        lenient().when(view.getInnerJoins()).thenReturn(Set.of());

        return view;
    }

    @SuppressWarnings("unchecked")
    private static LogicalView createSqlViewWithDatabaseSource(DatabaseSource dbSource, Set<ExpressionField> fields) {
        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getQuery()).thenReturn(null);
        lenient().when(logicalSource.getTableName()).thenReturn(null);
        lenient().when(logicalSource.getSource()).thenReturn(dbSource);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        var fieldSet = (Set<Field>) (Set<?>) fields;
        lenient().when(view.getFields()).thenReturn(fieldSet);
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());
        lenient().when(view.getLeftJoins()).thenReturn(Set.of());
        lenient().when(view.getInnerJoins()).thenReturn(Set.of());

        return view;
    }

    private static LogicalView createSqlViewWithTableAndJoins(
            String tableName,
            Set<ExpressionField> fields,
            Set<LogicalViewJoin> leftJoins,
            Set<LogicalViewJoin> innerJoins) {
        var view = createSqlViewWithTable(tableName, fields);
        lenient().when(view.getLeftJoins()).thenReturn(leftJoins);
        lenient().when(view.getInnerJoins()).thenReturn(innerJoins);
        return view;
    }

    private static LogicalView createSqlViewWithTableAndAnnotations(
            String tableName, Set<ExpressionField> fields, Set<StructuralAnnotation> annotations) {
        var view = createSqlViewWithTable(tableName, fields);
        lenient().when(view.getStructuralAnnotations()).thenReturn(annotations);
        return view;
    }

    private static LogicalView createSqlViewWithTableJoinsAndAnnotations(
            String tableName,
            Set<ExpressionField> fields,
            Set<LogicalViewJoin> leftJoins,
            Set<LogicalViewJoin> innerJoins,
            Set<StructuralAnnotation> annotations) {
        var view = createSqlViewWithTable(tableName, fields);
        lenient().when(view.getLeftJoins()).thenReturn(leftJoins);
        lenient().when(view.getInnerJoins()).thenReturn(innerJoins);
        lenient().when(view.getStructuralAnnotations()).thenReturn(annotations);
        return view;
    }

    @SuppressWarnings("unchecked")
    private static LogicalView createViewOnView(LogicalView innerView, Set<ExpressionField> fields) {
        var outerView = mock(LogicalView.class);
        lenient().when(outerView.getViewOn()).thenReturn(innerView);
        var fieldSet = (Set<Field>) (Set<?>) fields;
        lenient().when(outerView.getFields()).thenReturn(fieldSet);
        lenient().when(outerView.getResourceName()).thenReturn("outerView");
        lenient().when(outerView.getStructuralAnnotations()).thenReturn(Set.of());
        return outerView;
    }
}
