package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
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
import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.FunctionExecution;
import io.carml.model.IterableField;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.ParentMap;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.ReferenceFormulation;
import io.carml.model.StructuralAnnotation;
import io.carml.model.Template;
import io.carml.model.TriplesMap;
import io.carml.model.UniqueAnnotation;
import io.carml.model.impl.CarmlTemplate;
import io.carml.vocab.Rdf;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DuckDbViewCompilerTest {

    // --- JSON source tests ---

    @Nested
    class JsonSource {

        @Test
        void compile_jsonSourceWithFields_producesReadJsonAuto() {
            var view = createJsonView(
                    "people.json", null, Set.of(expressionField("name", "name"), expressionField("age", "age")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_json_auto('people.json')"));
            assertThat(sql, containsString("\"name\" \"name\""));
            assertThat(sql, containsString("\"age\" \"age\""));
            assertThat(sql, containsString("row_number() over () \"" + DuckDbViewCompiler.INDEX_COLUMN + "\""));
            assertThat(sql, containsString("with \"view_source\" as ("));
            assertThat(sql, containsString("from \"view_source\""));
        }

        @Test
        void compile_jsonSourceWithIterator_producesJsonExtractUnnest() {
            var view = createJsonView("data.json", "$.items[*]", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_text('data.json')"));
            assertThat(sql, containsString("json_extract(content, '$.items[*]')"));
            assertThat(sql, containsString("unnest("));
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", 'id') \"id\""));
        }

        @Test
        void compile_jsonSourceWithBlankIterator_omitsJsonPath() {
            var view = createJsonView("data.json", "   ", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_json_auto('data.json')"));
            assertThat(sql, not(containsString("json_path")));
        }

        @Test
        void compile_jsonSourceWithoutIterator_omitsJsonPath() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_json_auto('data.json')"));
            assertThat(sql, not(containsString("json_path")));
        }

        @Test
        void compile_jsonIteratorWithTemplate_producesJsonExtractStringConcat() {
            var segment1 = new CarmlTemplate.ExpressionSegment(0, "$.first");
            var segment2 = new CarmlTemplate.TextSegment(" ");
            var segment3 = new CarmlTemplate.ExpressionSegment(1, "$.last");
            var template = mock(Template.class);
            lenient().when(template.getSegments()).thenReturn(List.of(segment1, segment2, segment3));

            var field = mock(ExpressionField.class);
            lenient().when(field.getFieldName()).thenReturn("full_name");
            lenient().when(field.getReference()).thenReturn(null);
            lenient().when(field.getTemplate()).thenReturn(template);

            var view = createJsonView("data.json", "$.people[*]", Set.of(field, expressionField("id", "$.id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.first')"));
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.last')"));
            // DuckDB uses || for string concatenation
            assertThat(sql, containsString("||"));
            assertThat(sql, containsString("\"full_name\""));
        }
    }

    // --- JSON iterator with filter tests ---

    @Nested
    class JsonIteratorWithFilter {

        @Test
        void compile_jsonIteratorWithEqualStrFilter_producesWhereClause() {
            var view = createJsonView(
                    "data.json", "$.people[?(@.name == 'alice')]", Set.of(expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_text('data.json')"));
            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("json_extract_string(\"__iter\", '$.name') = 'alice'"));
        }

        @Test
        void compile_jsonIteratorWithGreaterThanFilter_producesWhereClause() {
            var view =
                    createJsonView("data.json", "$.items[?(@.price > 10)]", Set.of(expressionField("price", "price")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("cast(json_extract_string(\"__iter\", '$.price') as double) > 1E1"));
        }

        @Test
        void compile_jsonIteratorWithExistsFilter_producesIsNotNullClause() {
            var view = createJsonView("data.json", "$.items[?(@.type)]", Set.of(expressionField("type", "type")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("json_extract(\"__iter\", '$.type') is not null"));
        }

        @Test
        void compile_jsonIteratorWithRegexFilter_producesRegexpMatches() {
            var view = createJsonView(
                    "data.json", "$.items[?(@.name =~ /^test/)]", Set.of(expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("regexp_matches(json_extract_string(\"__iter\", '$.name'), '^test')"));
        }

        @Test
        void compile_jsonIteratorWithEqualNumFilter_producesWhereClause() {
            var view =
                    createJsonView("data.json", "$.items[?(@.count == 5)]", Set.of(expressionField("count", "count")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("cast(json_extract_string(\"__iter\", '$.count') as double) = 5E0"));
        }

        @Test
        void compile_jsonIteratorWithEqualBoolFilter_producesWhereClause() {
            var view = createJsonView(
                    "data.json", "$.items[?(@.active == true)]", Set.of(expressionField("active", "active")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("cast(json_extract_string(\"__iter\", '$.active') as boolean) = true"));
        }

        @Test
        void compile_jsonIteratorWithLessThanFilter_producesWhereClause() {
            var view =
                    createJsonView("data.json", "$.items[?(@.price < 20)]", Set.of(expressionField("price", "price")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("cast(json_extract_string(\"__iter\", '$.price') as double) < 2E1"));
        }
    }

    // --- CSV source tests ---

    @Nested
    class CsvSource {

        @Test
        void compile_csvSource_producesReadCsvAuto() {
            var view = createCsvView("data.csv", Set.of(expressionField("col1", "column1")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_csv_auto('data.csv')"));
            assertThat(sql, containsString("\"column1\" \"col1\""));
        }
    }

    // --- Parquet detection tests ---

    @Nested
    class ParquetDetection {

        @Test
        void compile_jsonSourceWithParquetExtension_producesReadParquet() {
            var view = createJsonView("data.parquet", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_parquet('data.parquet')"));
            assertThat(sql, not(containsString("read_json_auto")));
        }

        @Test
        void compile_csvSourceWithParquetExtension_producesReadParquet() {
            var view = createCsvView("data.parq", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_parquet('data.parq')"));
            assertThat(sql, not(containsString("read_csv_auto")));
        }
    }

    // --- SQL source tests ---

    @Nested
    class SqlSource {

        @Test
        void compile_sqlSourceWithQuery_producesSubquery() {
            var view = createSqlViewWithQuery(
                    "SELECT id, name FROM users", Set.of(expressionField("id", "id"), expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("(SELECT id, name FROM users)"));
        }

        @Test
        void compile_sqlSourceWithTableName_producesTableReference() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("select * from \"users\""));
        }

        @Test
        void compile_sqlSourceWithDatabaseSourceFallback_producesSubquery() {
            var dbSource = mock(DatabaseSource.class);
            when(dbSource.getQuery()).thenReturn("SELECT * FROM orders");

            var view = createSqlViewWithDatabaseSource(dbSource, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("(SELECT * FROM orders)"));
        }
    }

    // --- Column projection tests ---

    @Nested
    class ColumnProjection {

        @Test
        void compile_withProjectedFields_selectsOnlyProjectedFields() {
            var view = createJsonView(
                    "data.json",
                    null,
                    Set.of(
                            expressionField("name", "name"),
                            expressionField("age", "age"),
                            expressionField("email", "email")));
            var context = EvaluationContext.withProjectedFields(Set.of("name"));

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"name\" \"name\""));
            assertThat(sql, not(containsString("\"age\" \"age\"")));
            assertThat(sql, not(containsString("\"email\" \"email\"")));
        }

        @Test
        void compile_withEmptyProjectedFields_selectsAllFields() {
            var view = createJsonView(
                    "data.json", null, Set.of(expressionField("name", "name"), expressionField("age", "age")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"name\" \"name\""));
            assertThat(sql, containsString("\"age\" \"age\""));
        }
    }

    // --- Dedup strategy tests ---

    @Nested
    class Deduplication {

        @Test
        void compile_withNoneDedupStrategy_omitsDistinct() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, not(containsString("select distinct")));
            assertThat(sql, not(containsString("\"deduped\"")));
        }

        @Test
        void compile_withExactDedupStrategy_usesDistinctCte() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"deduped\" as ("));
            assertThat(sql, containsString("select distinct"));
            assertThat(sql, containsString("from \"deduped\""));
            // ROW_NUMBER assigned after dedup in the outer SELECT
            assertThat(sql, containsString("select *, row_number() over () \"__idx\""));
        }

        @Test
        void compile_withSimpleEqualityDedupStrategy_usesDistinctCte() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.simpleEquality(), null);

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"deduped\" as ("));
            assertThat(sql, containsString("select distinct"));
        }
    }

    // --- Limit tests ---

    @Nested
    class Limiting {

        @Test
        void compile_withLimit_appendsLimitClause() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.withProjectedFieldsAndLimit(Set.of(), 100L);

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("limit 100"));
        }

        @Test
        void compile_withoutLimit_omitsLimitClause() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, not(containsString("limit")));
        }
    }

    // --- Index column tests ---

    @Nested
    class IndexColumn {

        @Test
        void compile_alwaysIncludesIndexColumn() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("row_number() over () \"__idx\""));
        }
    }

    // --- Template expression tests ---

    @Nested
    class TemplateExpression {

        @Test
        void compile_fieldWithSingleSegmentTemplate_producesColumnReference() {
            var template = CarmlTemplate.of(List.of(new CarmlTemplate.ExpressionSegment(0, "id")));

            var field = mock(ExpressionField.class);
            when(field.getFieldName()).thenReturn("identifier");
            when(field.getReference()).thenReturn(null);
            when(field.getTemplate()).thenReturn(template);

            var view = createJsonView("data.json", null, Set.of(field));
            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults());

            assertThat(sql, containsString("\"id\" \"identifier\""));
            assertThat(sql, not(containsString("||")));
        }

        @Test
        void compile_fieldWithTemplate_producesConcatenation() {
            var template = CarmlTemplate.of(List.of(
                    new CarmlTemplate.TextSegment("http://example.org/"),
                    new CarmlTemplate.ExpressionSegment(0, "id")));

            var field = mock(ExpressionField.class);
            when(field.getFieldName()).thenReturn("uri");
            when(field.getReference()).thenReturn(null);
            when(field.getTemplate()).thenReturn(template);

            var view = createJsonView("data.json", null, Set.of(field));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("'http://example.org/'"));
            assertThat(sql, containsString("||"));
            assertThat(sql, containsString("\"id\""));
            assertThat(sql, containsString("\"uri\""));
        }
    }

    // --- Constant expression tests ---

    @Nested
    class ConstantExpression {

        @Test
        void compile_fieldWithConstant_producesLiteral() {
            var constantValue = SimpleValueFactory.getInstance().createLiteral("fixed_value");

            var field = mock(ExpressionField.class);
            when(field.getFieldName()).thenReturn("status");
            when(field.getReference()).thenReturn(null);
            when(field.getTemplate()).thenReturn(null);
            when(field.getConstant()).thenReturn(constantValue);

            var view = createJsonView("data.json", null, Set.of(field));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("'fixed_value' \"status\""));
        }
    }

    // --- Error handling tests ---

    @Nested
    class ErrorHandling {

        @Test
        void compile_viewOnView_throwsUnsupportedOperationException() {
            var nestedView = mock(LogicalView.class);
            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(nestedView);

            var context = EvaluationContext.defaults();

            assertThrows(UnsupportedOperationException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_noReferenceFormulation_throwsIllegalArgumentException() {
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(null);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);

            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_xpathReferenceFormulation_throwsIllegalArgumentException() {
            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.XPath);

            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);

            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_fileSourceWithNullUrl_throwsIllegalArgumentException() {
            var fileSource = mock(FileSource.class);
            when(fileSource.getUrl()).thenReturn(null);

            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            when(logicalSource.getSource()).thenReturn(fileSource);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);

            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_nullSource_throwsIllegalArgumentException() {
            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            when(logicalSource.getSource()).thenReturn(null);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);

            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_unsupportedViewOnType_throwsIllegalArgumentException() {
            var unknownTarget = mock(AbstractLogicalSource.class);
            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(unknownTarget);
            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_unknownReferenceFormulation_throwsIllegalArgumentException() {
            var unknownIri = SimpleValueFactory.getInstance().createIRI("http://example.org/unknown");
            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(unknownIri);

            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);
            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_fieldWithFunctionValue_throwsUnsupportedOperationException() {
            var field = mock(ExpressionField.class);
            when(field.getFieldName()).thenReturn("computed");
            when(field.getReference()).thenReturn(null);
            when(field.getTemplate()).thenReturn(null);
            when(field.getConstant()).thenReturn(null);
            when(field.getFunctionValue()).thenReturn(mock(TriplesMap.class));

            var view = createJsonView("data.json", null, Set.of(field));
            var context = EvaluationContext.defaults();

            assertThrows(UnsupportedOperationException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_fieldWithFunctionExecution_throwsUnsupportedOperationException() {
            var field = mock(ExpressionField.class);
            when(field.getFieldName()).thenReturn("computed");
            when(field.getReference()).thenReturn(null);
            when(field.getTemplate()).thenReturn(null);
            when(field.getConstant()).thenReturn(null);
            when(field.getFunctionValue()).thenReturn(null);
            when(field.getFunctionExecution()).thenReturn(mock(FunctionExecution.class));

            var view = createJsonView("data.json", null, Set.of(field));
            var context = EvaluationContext.defaults();

            assertThrows(UnsupportedOperationException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_fieldWithNoExpression_throwsIllegalArgumentException() {
            var field = mock(ExpressionField.class);
            when(field.getFieldName()).thenReturn("broken");
            when(field.getReference()).thenReturn(null);
            when(field.getTemplate()).thenReturn(null);
            when(field.getConstant()).thenReturn(null);
            when(field.getFunctionValue()).thenReturn(null);
            when(field.getFunctionExecution()).thenReturn(null);

            var view = createJsonView("data.json", null, Set.of(field));
            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_sqlSourceWithNoQueryOrTable_throwsIllegalArgumentException() {
            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.Rdb);

            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            when(logicalSource.getQuery()).thenReturn(null);
            when(logicalSource.getTableName()).thenReturn(null);
            when(logicalSource.getSource()).thenReturn(null);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);

            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_iterableFieldWithNullIterator_throwsIllegalArgumentException() {
            var nestedField = expressionField("item_type", "type");
            var iterableField = iterableField("items", null, Set.of(nestedField));
            var view = createJsonViewWithFields("data.json", null, Set.of(iterableField));
            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_iterableNestedFieldWithNoReference_throwsUnsupportedOperationException() {
            var nestedField = mock(ExpressionField.class);
            when(nestedField.getFieldName()).thenReturn("item_type");
            when(nestedField.getReference()).thenReturn(null);
            var iterableField = iterableField("items", "items", Set.of(nestedField));
            var view = createJsonViewWithFields("data.json", null, Set.of(iterableField));
            var context = EvaluationContext.defaults();

            assertThrows(UnsupportedOperationException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_joinWithNoConditions_throwsIllegalArgumentException() {
            var parentView = createJsonView("departments.json", null, Set.of(expressionField("dept_id", "id")));
            var viewJoin = logicalViewJoin(parentView, Set.of(), Set.of(expressionField("dept_name", "name")));
            var view = createJsonViewWithJoins(
                    "employees.json", null, Set.of(expressionField("name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_joinProjectedFieldWithNoReference_throwsUnsupportedOperationException() {
            var parentView = createJsonView("departments.json", null, Set.of(expressionField("dept_id", "id")));
            var brokenField = mock(ExpressionField.class);
            when(brokenField.getFieldName()).thenReturn("dept_name");
            when(brokenField.getReference()).thenReturn(null);
            var viewJoin =
                    logicalViewJoin(parentView, Set.of(joinCondition("dept_id", "dept_id")), Set.of(brokenField));
            var view = createJsonViewWithJoins(
                    "employees.json", null, Set.of(expressionField("name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            assertThrows(UnsupportedOperationException.class, () -> DuckDbViewCompiler.compile(view, context));
        }
    }

    // --- FilePath source tests ---

    @Nested
    class FilePathSource {

        @Test
        void compile_filePathSource_resolvesPath() {
            var fields = Set.<Field>of(expressionField("id", "id"));

            var filePath = mock(FilePath.class);
            when(filePath.getPath()).thenReturn("data/people.json");

            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            when(logicalSource.getSource()).thenReturn(filePath);
            when(logicalSource.getIterator()).thenReturn(null);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);
            when(view.getFields()).thenReturn(fields);
            lenient().when(view.getResourceName()).thenReturn("testView");

            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_json_auto('data/people.json')"));
        }
    }

    // --- SQL escaping tests ---

    @Nested
    class SqlEscaping {

        @Test
        void compile_fieldReferenceWithSpecialChars_escapesDoubleQuotes() {
            var view = createJsonView("data.json", null, Set.of(expressionField("col", "my\"column")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"my\"\"column\" \"col\""));
        }

        @Test
        void compile_filePathWithSingleQuote_escapesSingleQuotes() {
            var view = createJsonView("data's.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_json_auto('data''s.json')"));
        }

        @Test
        void compile_fieldNameWithSpecialChars_quotesAlias() {
            var view = createJsonView("data.json", null, Set.of(expressionField("my-field", "ref")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"ref\" \"my-field\""));
        }

        @Test
        void compile_fieldNameWithAlphanumericChars_unquotedAlias() {
            var view = createJsonView("data.json", null, Set.of(expressionField("myField_1", "ref")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            // jOOQ may quote unquoted names depending on dialect; verify the alias is present
            assertThat(sql, containsString("\"ref\""));
            assertThat(sql, containsString("myField_1"));
        }
    }

    // --- Combined features tests ---

    @Nested
    class CombinedFeatures {

        @Test
        void compile_withDistinctAndLimit_includesBoth() {
            var view = createJsonView(
                    "data.json", null, Set.of(expressionField("name", "name"), expressionField("age", "age")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), 100L);

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"deduped\" as ("));
            assertThat(sql, containsString("select distinct"));
            assertThat(sql, containsString("limit 100"));
            assertThat(sql, containsString("select *, row_number() over () \"__idx\""));
        }

        @Test
        void compile_withProjectionAndLimit_selectsProjectedFieldsAndLimits() {
            var view = createJsonView(
                    "data.json",
                    null,
                    Set.of(
                            expressionField("name", "name"),
                            expressionField("age", "age"),
                            expressionField("email", "email")));
            var context = EvaluationContext.withProjectedFieldsAndLimit(Set.of("name", "email"), 50L);

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"name\" \"name\""));
            assertThat(sql, containsString("\"email\" \"email\""));
            assertThat(sql, not(containsString("\"age\" \"age\"")));
            assertThat(sql, containsString("limit 50"));
        }
    }

    // --- RML vocabulary variant tests ---

    @Nested
    class RmlVocabularyVariants {

        @Test
        void compile_rmlJsonPathFormulation_producesReadJsonAuto() {
            var view = createViewWithRefFormulation(Rdf.Rml.JsonPath, "data.json", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_json_auto('data.json')"));
        }

        @Test
        void compile_rmlSQL2008TableFormulation_producesSqlSource() {
            var view = createSqlViewWithQueryAndFormulation(
                    Rdf.Rml.SQL2008Table, null, "users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("select * from \"users\""));
        }

        @Test
        void compile_rmlSQL2008QueryFormulation_producesSqlSource() {
            var view = createSqlViewWithQueryAndFormulation(
                    Rdf.Rml.SQL2008Query, "SELECT * FROM people", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("(SELECT * FROM people)"));
        }

        @Test
        void compile_rmlXPathFormulation_throwsIllegalArgumentException() {
            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(Rdf.Rml.XPath);

            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);
            var context = EvaluationContext.defaults();

            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_rmlCsvFormulation_producesReadCsvAuto() {
            var view = createViewWithRefFormulation(Rdf.Rml.Csv, "data.csv", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_csv_auto('data.csv')"));
        }
    }

    // --- SQL structure verification ---

    @Nested
    class SqlStructure {

        @Test
        void compile_producesWellFormedCte() {
            var view = createJsonView("people.json", null, Set.of(expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            var expected = "with \"view_source\" as ("
                    + "select * from read_json_auto('people.json')"
                    + ") select \"view_source\".\"name\" \"name\", "
                    + "row_number() over () \"__idx\" "
                    + "from \"view_source\"";

            assertThat(sql, is(expected));
        }

        @Test
        void compile_withDistinctAndLimit_producesCorrectStructure() {
            var view = createJsonView("people.json", null, Set.of(expressionField("name", "name")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), 10L);

            var sql = DuckDbViewCompiler.compile(view, context);

            var expected = "with \"view_source\" as ("
                    + "select * from read_json_auto('people.json')"
                    + "), \"deduped\" as ("
                    + "select distinct \"view_source\".\"name\" \"name\" "
                    + "from \"view_source\""
                    + ") select *, "
                    + "row_number() over () \"__idx\" "
                    + "from \"deduped\" "
                    + "limit 10";

            assertThat(sql, is(expected));
        }
    }

    // --- IterableField / UNNEST compilation tests ---

    @Nested
    class IterableFieldCompilation {

        @Test
        void compile_iterableFieldWithNestedExpressionFields_producesUnnest() {
            var nestedField = expressionField("item_type", "type");
            var iterableField = iterableField("item", "items", Set.of(nestedField));

            var view = createJsonViewWithFields("data.json", null, Set.of(iterableField));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(\"view_source\".\"items\")"));
            assertThat(sql, containsString("\"item\""));
            assertThat(sql, containsString("\"item\".\"type\" \"item.item_type\""));
            // Ordinal column for iterable field index
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
        }

        @Test
        void compile_iterableFieldWithMultipleNestedFields_producesUnnest() {
            var nestedType = expressionField("item_type", "type");
            var nestedName = expressionField("item_name", "name");
            var iterableField = iterableField("item", "items", Set.of(nestedType, nestedName));

            var view = createJsonViewWithFields("data.json", null, Set.of(iterableField));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(\"view_source\".\"items\")"));
            assertThat(sql, containsString("\"item\".\"type\" \"item.item_type\""));
            assertThat(sql, containsString("\"item\".\"name\" \"item.item_name\""));
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
        }

        @Test
        void compile_mixedExpressionAndIterableFields_producesCorrectSql() {
            var topLevelField = expressionField("person_name", "name");
            var nestedField = expressionField("item_type", "type");
            var iterableField = iterableField("item", "items", Set.of(nestedField));

            // Use LinkedHashSet to preserve order for deterministic assertion
            var fields = new LinkedHashSet<Field>();
            fields.add(topLevelField);
            fields.add(iterableField);

            var view = createJsonViewWithFields("data.json", null, fields);
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            // Top-level expression field
            assertThat(sql, containsString("\"name\" \"person_name\""));
            // UNNEST for iterable field via LATERAL subquery
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(\"view_source\".\"items\")"));
            // Nested field qualified by unnest alias
            assertThat(sql, containsString("\"item\".\"type\" \"item.item_type\""));
            // Ordinal column
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
            // FROM clause has both view_source and LATERAL
            assertThat(sql, containsString("from \"view_source\", LATERAL"));
        }

        @Test
        void compile_iterableFieldWithProjection_selectsOnlyProjectedNestedFields() {
            var nested1 = expressionField("item_type", "type");
            var nested2 = expressionField("item_name", "name");
            var iterableField = iterableField("item", "items", Set.of(nested1, nested2));

            var view = createJsonViewWithFields("data.json", null, Set.of(iterableField));
            var context = EvaluationContext.withProjectedFields(Set.of("item.item_type"));

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"item\".\"type\" \"item.item_type\""));
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
            assertThat(sql, not(containsString("\"item\".\"name\" \"item.item_name\"")));
        }

        @Test
        void compile_iterableFieldWithDedup_producesUnnestInDistinctCte() {
            var nestedField = expressionField("item_type", "type");
            var iterableField = iterableField("item", "items", Set.of(nestedField));

            var view = createJsonViewWithFields("data.json", null, Set.of(iterableField));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"deduped\" as ("));
            assertThat(sql, containsString("select distinct"));
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(\"view_source\".\"items\")"));
            assertThat(sql, containsString("\"item\".\"type\" \"item.item_type\""));
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
        }

        @Test
        void compile_jsonIteratorWithIterableField_producesJsonExtractUnnest() {
            var nestedField = expressionField("contact_phone", "$.phone");
            var iterable = iterableField("contacts", "$.contacts[*]", Set.of(nestedField));

            var fields = new LinkedHashSet<Field>();
            fields.add(expressionField("name", "$.name"));
            fields.add(iterable);

            var view = createJsonViewWithFields("data.json", "$.people[*]", fields);
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            // Source uses read_text + json_extract + unnest for iterator
            assertThat(sql, containsString("read_text('data.json')"));
            assertThat(sql, containsString("json_extract(content, '$.people[*]')"));
            // Top-level field uses json_extract_string from __iter
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.name') \"name\""));
            // Iterable field uses LATERAL subquery with json_extract and ordinals
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(json_extract(\"view_source\".\"__iter\", '$.contacts[*]'))"));
            // Nested field uses json_extract_string from unnest field
            assertThat(
                    sql,
                    containsString(
                            "json_extract_string(\"contacts\".\"unnest\", '$.phone') \"contacts.contact_phone\""));
            // Ordinal column for iterable field index
            assertThat(sql, containsString("\"contacts\".\"__ord\" \"contacts.#\""));
        }
    }

    // --- LogicalViewJoin / SQL JOIN compilation tests ---

    @Nested
    class JoinCompilation {

        @Test
        void compile_leftJoinWithSingleCondition_producesLeftJoin() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCondition = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCondition), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "employees.json", null, Set.of(expressionField("emp_name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("left outer join"));
            assertThat(sql, containsString("\"parent_0\""));
            assertThat(sql, containsString("\"view_source\".\"dept_id\" = \"parent_0\".\"dept_id\""));
            assertThat(sql, containsString("\"parent_0\".\"dept_name\" \"department_name\""));
        }

        @Test
        void compile_innerJoinWithSingleCondition_producesInnerJoin() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCondition = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCondition), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "employees.json", null, Set.of(expressionField("emp_name", "name")), Set.of(), Set.of(viewJoin));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("join ("));
            assertThat(sql, not(containsString("left outer join")));
            assertThat(sql, containsString("\"parent_0\""));
            assertThat(sql, containsString("\"view_source\".\"dept_id\" = \"parent_0\".\"dept_id\""));
        }

        @Test
        void compile_multipleJoinConditions_producesCompoundOnClause() {
            var parentView = createJsonView(
                    "details.json", null, Set.of(expressionField("id", "id"), expressionField("region", "region")));

            var joinField = expressionField("detail_region", "region");
            var condition1 = joinCondition("id", "id");
            var condition2 = joinCondition("region", "region");
            var viewJoin = logicalViewJoin(parentView, Set.of(condition1, condition2), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "main.json", null, Set.of(expressionField("name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            // Both conditions should appear in the ON clause
            assertThat(sql, containsString("\"view_source\".\"id\" = \"parent_0\".\"id\""));
            assertThat(sql, containsString("\"view_source\".\"region\" = \"parent_0\".\"region\""));
            // They should be ANDed together
            assertThat(sql, containsString("and"));
        }

        @Test
        void compile_joinWithProjectedFields_selectsJoinFields() {
            var parentView = createJsonView(
                    "countries.json",
                    null,
                    Set.of(expressionField("code", "code"), expressionField("country_name", "name")));

            var projectedField1 = expressionField("country_code", "code");
            var projectedField2 = expressionField("country_name", "country_name");
            var joinCondition = joinCondition("country_code", "code");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCondition), Set.of(projectedField1, projectedField2));

            var view = createJsonViewWithJoins(
                    "cities.json", null, Set.of(expressionField("city_name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("\"parent_0\".\"code\" \"country_code\""));
            assertThat(sql, containsString("\"parent_0\".\"country_name\" \"country_name\""));
        }

        @Test
        void compile_multipleJoins_producesMultipleJoinClauses() {
            var parentView1 = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));
            var parentView2 = createJsonView(
                    "offices.json",
                    null,
                    Set.of(expressionField("office_id", "id"), expressionField("office_city", "city")));

            var joinField1 = expressionField("department_name", "dept_name");
            var joinCondition1 = joinCondition("dept_id", "dept_id");
            var viewJoin1 = logicalViewJoin(parentView1, Set.of(joinCondition1), Set.of(joinField1));

            var joinField2 = expressionField("office_city", "office_city");
            var joinCondition2 = joinCondition("office_id", "office_id");
            var viewJoin2 = logicalViewJoin(parentView2, Set.of(joinCondition2), Set.of(joinField2));

            // Use LinkedHashSet to preserve ordering for deterministic aliases
            var leftJoins = new LinkedHashSet<LogicalViewJoin>();
            leftJoins.add(viewJoin1);
            leftJoins.add(viewJoin2);

            var view = createJsonViewWithJoins(
                    "employees.json", null, Set.of(expressionField("emp_name", "name")), leftJoins, Set.of());
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            // Two left joins with sequential parent aliases
            assertThat(sql, containsString("\"parent_0\""));
            assertThat(sql, containsString("\"parent_1\""));
            assertThat(sql, containsString("\"parent_0\".\"dept_name\" \"department_name\""));
            assertThat(sql, containsString("\"parent_1\".\"office_city\" \"office_city\""));
        }

        @Test
        void compile_jsonIteratorWithJoin_producesJsonExtractStringForJoinCondition() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            // Child uses JSON with iterator, including dept_id field for join resolution
            var view = createJsonViewWithJoins(
                    "employees.json",
                    "$.employees[*]",
                    Set.of(expressionField("emp_name", "$.name"), expressionField("dept_id", "$.dept_id")),
                    Set.of(viewJoin),
                    Set.of());
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            // Source uses json_extract for iterator
            assertThat(sql, containsString("read_text('employees.json')"));
            assertThat(sql, containsString("json_extract(content, '$.employees[*]')"));
            // Top-level field uses json_extract_string from __iter
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.name') \"emp_name\""));
            // Join child side uses json_extract_string (resolved via fieldNameToRefMap)
            assertThat(
                    sql,
                    containsString(
                            "json_extract_string(\"view_source\".\"__iter\", '$.dept_id') = \"parent_0\".\"dept_id\""));
            assertThat(sql, containsString("left outer join"));
        }

        @Test
        void compile_iterableFieldAndJoin_producesUnnestAndJoin() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var nestedField = expressionField("item_type", "type");
            var iterableField = iterableField("item", "items", Set.of(nestedField));

            var fields = new LinkedHashSet<Field>();
            fields.add(expressionField("emp_name", "name"));
            fields.add(iterableField);

            var view = createJsonViewWithFieldsAndJoins("employees.json", null, fields, Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(\"view_source\".\"items\")"));
            assertThat(sql, containsString("left outer join"));
            assertThat(sql, containsString("\"item\".\"type\" \"item.item_type\""));
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
            assertThat(sql, containsString("\"parent_0\".\"dept_name\" \"department_name\""));
        }
    }

    // --- Helper methods ---

    private static ExpressionField expressionField(String fieldName, String reference) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(reference);
        return field;
    }

    @SuppressWarnings("unchecked")
    private static IterableField iterableField(String fieldName, String iterator, Set<ExpressionField> nestedFields) {
        var field = mock(IterableField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getIterator()).thenReturn(iterator);
        lenient().when(field.getFields()).thenReturn((Set<Field>) (Set<?>) nestedFields);
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

    private static LogicalView createJsonView(String fileName, String iterator, Set<ExpressionField> fields) {
        return createViewWithRefFormulationAndIterator(Rdf.Ql.JsonPath, fileName, iterator, fields);
    }

    private static LogicalView createJsonViewWithFields(String fileName, String iterator, Set<Field> fields) {
        return createViewWithRefFormulationAndIteratorMixed(Rdf.Ql.JsonPath, fileName, iterator, fields);
    }

    private static LogicalView createJsonViewWithJoins(
            String fileName,
            String iterator,
            Set<ExpressionField> fields,
            Set<LogicalViewJoin> leftJoins,
            Set<LogicalViewJoin> innerJoins) {
        var view = createViewWithRefFormulationAndIterator(Rdf.Ql.JsonPath, fileName, iterator, fields);
        lenient().when(view.getLeftJoins()).thenReturn(leftJoins);
        lenient().when(view.getInnerJoins()).thenReturn(innerJoins);
        return view;
    }

    private static LogicalView createJsonViewWithFieldsAndJoins(
            String fileName,
            String iterator,
            Set<Field> fields,
            Set<LogicalViewJoin> leftJoins,
            Set<LogicalViewJoin> innerJoins) {
        var view = createViewWithRefFormulationAndIteratorMixed(Rdf.Ql.JsonPath, fileName, iterator, fields);
        lenient().when(view.getLeftJoins()).thenReturn(leftJoins);
        lenient().when(view.getInnerJoins()).thenReturn(innerJoins);
        return view;
    }

    private static LogicalView createCsvView(String fileName, Set<ExpressionField> fields) {
        return createViewWithRefFormulationAndIterator(Rdf.Ql.Csv, fileName, null, fields);
    }

    private static LogicalView createViewWithRefFormulation(
            org.eclipse.rdf4j.model.Resource refIri, String fileName, Set<ExpressionField> fields) {
        return createViewWithRefFormulationAndIterator(refIri, fileName, null, fields);
    }

    @SuppressWarnings("unchecked")
    private static LogicalView createViewWithRefFormulationAndIterator(
            org.eclipse.rdf4j.model.Resource refIri, String fileName, String iterator, Set<ExpressionField> fields) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn(fileName);

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getSource()).thenReturn(fileSource);
        lenient().when(logicalSource.getIterator()).thenReturn(iterator);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        var fieldSet = (Set<Field>) (Set<?>) fields;
        lenient().when(view.getFields()).thenReturn(fieldSet);
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }

    private static LogicalView createViewWithRefFormulationAndIteratorMixed(
            Resource refIri, String fileName, String iterator, Set<Field> fields) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn(fileName);

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getSource()).thenReturn(fileSource);
        lenient().when(logicalSource.getIterator()).thenReturn(iterator);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        lenient().when(view.getFields()).thenReturn(fields);
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }

    private static LogicalView createSqlViewWithQuery(String query, Set<ExpressionField> fields) {
        var refFormulation = mock(ReferenceFormulation.class);
        when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.Rdb);

        var logicalSource = mock(LogicalSource.class);
        when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        when(logicalSource.getQuery()).thenReturn(query);

        var view = mock(LogicalView.class);
        when(view.getViewOn()).thenReturn(logicalSource);
        when(view.getFields()).thenReturn(castFieldSet(fields));
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }

    private static LogicalView createSqlViewWithTable(String tableName, Set<ExpressionField> fields) {
        var refFormulation = mock(ReferenceFormulation.class);
        when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.Rdb);

        var logicalSource = mock(LogicalSource.class);
        when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        when(logicalSource.getQuery()).thenReturn(null);
        when(logicalSource.getTableName()).thenReturn(tableName);

        var view = mock(LogicalView.class);
        when(view.getViewOn()).thenReturn(logicalSource);
        when(view.getFields()).thenReturn(castFieldSet(fields));
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }

    private static LogicalView createSqlViewWithDatabaseSource(DatabaseSource dbSource, Set<ExpressionField> fields) {
        var refFormulation = mock(ReferenceFormulation.class);
        when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.Rdb);

        var logicalSource = mock(LogicalSource.class);
        when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        when(logicalSource.getQuery()).thenReturn(null);
        when(logicalSource.getTableName()).thenReturn(null);
        when(logicalSource.getSource()).thenReturn(dbSource);

        var view = mock(LogicalView.class);
        when(view.getViewOn()).thenReturn(logicalSource);
        when(view.getFields()).thenReturn(castFieldSet(fields));
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }

    private static LogicalView createSqlViewWithQueryAndFormulation(
            org.eclipse.rdf4j.model.Resource refIri, String query, String tableName, Set<ExpressionField> fields) {
        var refFormulation = mock(ReferenceFormulation.class);
        when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = mock(LogicalSource.class);
        when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        when(logicalSource.getQuery()).thenReturn(query);
        lenient().when(logicalSource.getTableName()).thenReturn(tableName);

        var view = mock(LogicalView.class);
        when(view.getViewOn()).thenReturn(logicalSource);
        when(view.getFields()).thenReturn(castFieldSet(fields));
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }

    @SuppressWarnings("unchecked")
    private static Set<Field> castFieldSet(Set<ExpressionField> fields) {
        return (Set<Field>) (Set<?>) fields;
    }

    // --- Annotation mock helpers ---

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

    private static LogicalView createJsonViewWithAnnotations(
            String fileName, String iterator, Set<ExpressionField> fields, Set<StructuralAnnotation> annotations) {
        var view = createViewWithRefFormulationAndIterator(Rdf.Ql.JsonPath, fileName, iterator, fields);
        lenient().when(view.getStructuralAnnotations()).thenReturn(annotations);
        return view;
    }

    private static LogicalView createJsonViewWithJoinsAndAnnotations(
            String fileName,
            String iterator,
            Set<ExpressionField> fields,
            Set<LogicalViewJoin> leftJoins,
            Set<LogicalViewJoin> innerJoins,
            Set<StructuralAnnotation> annotations) {
        var view = createViewWithRefFormulationAndIterator(Rdf.Ql.JsonPath, fileName, iterator, fields);
        lenient().when(view.getLeftJoins()).thenReturn(leftJoins);
        lenient().when(view.getInnerJoins()).thenReturn(innerJoins);
        lenient().when(view.getStructuralAnnotations()).thenReturn(annotations);
        return view;
    }

    // --- Structural annotation optimization tests ---

    @Nested
    class AnnotationOptimizations {

        @Test
        void compile_primaryKeyCoveringAllFields_omitsDistinct() {
            var idField = expressionField("id", "id");
            var nameField = expressionField("name", "name");

            var pkAnnotation = primaryKeyAnnotation(idField, nameField);
            var view =
                    createJsonViewWithAnnotations("data.json", null, Set.of(idField, nameField), Set.of(pkAnnotation));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            var sql = DuckDbViewCompiler.compile(view, context);

            // PK covers all selected fields, so DISTINCT should be omitted
            assertThat(sql, not(containsString("select distinct")));
            assertThat(sql, not(containsString("\"deduped\"")));
        }

        @Test
        void compile_uniquePlusNotNullCoveringAllFields_omitsDistinct() {
            var idField = expressionField("id", "id");
            var nameField = expressionField("name", "name");

            var uniqueAnnotation = uniqueAnnotation(idField);
            var notNullAnnotation = notNullAnnotation(idField);
            var view = createJsonViewWithAnnotations(
                    "data.json", null, Set.of(idField, nameField), Set.of(uniqueAnnotation, notNullAnnotation));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            var sql = DuckDbViewCompiler.compile(view, context);

            // Unique + NotNull covers all selected fields (unique field "id" is in selected fields
            // and is NotNull), so DISTINCT should be omitted
            assertThat(sql, not(containsString("select distinct")));
            assertThat(sql, not(containsString("\"deduped\"")));
        }

        @Test
        void compile_uniqueWithoutNotNull_retainsDistinct() {
            var idField = expressionField("id", "id");
            var nameField = expressionField("name", "name");

            // Unique on "id" but no NotNull — cannot skip DISTINCT
            var uniqueAnnotation = uniqueAnnotation(idField);
            var view = createJsonViewWithAnnotations(
                    "data.json", null, Set.of(idField, nameField), Set.of(uniqueAnnotation));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            var sql = DuckDbViewCompiler.compile(view, context);

            // Without NotNull, Unique alone is insufficient — DISTINCT must remain
            assertThat(sql, containsString("select distinct"));
            assertThat(sql, containsString("\"deduped\""));
        }

        @Test
        void compile_foreignKeyNotProjected_eliminatesJoin() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            // FK annotation from child field "dept_id" to the parent view
            var childFkField = expressionField("dept_id", "dept_id");
            var parentFkField = expressionField("dept_id", "id");
            var fkAnnotation = foreignKeyAnnotation(parentView, List.of(childFkField), List.of(parentFkField));

            // Only project "emp_name" — the join's "department_name" is NOT projected
            var view = createJsonViewWithJoinsAndAnnotations(
                    "employees.json",
                    null,
                    Set.of(expressionField("emp_name", "name")),
                    Set.of(viewJoin),
                    Set.of(),
                    Set.of(fkAnnotation));
            var context = EvaluationContext.withProjectedFields(Set.of("emp_name"));

            var sql = DuckDbViewCompiler.compile(view, context);

            // FK guarantees referential integrity and parent fields are not needed
            assertThat(sql, not(containsString("left outer join")));
            assertThat(sql, not(containsString("join (")));
            assertThat(sql, not(containsString("\"parent_0\"")));
        }

        @Test
        void compile_foreignKeyWithProjectedParentFields_retainsJoin() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var childFkField = expressionField("dept_id", "dept_id");
            var parentFkField = expressionField("dept_id", "id");
            var fkAnnotation = foreignKeyAnnotation(parentView, List.of(childFkField), List.of(parentFkField));

            // Project both "emp_name" AND "department_name" — parent field IS projected
            var view = createJsonViewWithJoinsAndAnnotations(
                    "employees.json",
                    null,
                    Set.of(expressionField("emp_name", "name")),
                    Set.of(viewJoin),
                    Set.of(),
                    Set.of(fkAnnotation));
            var context = EvaluationContext.withProjectedFields(Set.of("emp_name", "department_name"));

            var sql = DuckDbViewCompiler.compile(view, context);

            // Parent fields are projected — join must be retained
            assertThat(sql, containsString("left outer join"));
            assertThat(sql, containsString("\"parent_0\""));
        }

        @Test
        void compile_notNullOnChildJoinKeys_upgradesLeftToInner() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            // NotNull on the child join key field "dept_id"
            var deptIdField = expressionField("dept_id", "dept_id");
            var notNullAnnotation = notNullAnnotation(deptIdField);

            var view = createJsonViewWithJoinsAndAnnotations(
                    "employees.json",
                    null,
                    Set.of(expressionField("emp_name", "name")),
                    Set.of(viewJoin),
                    Set.of(),
                    Set.of(notNullAnnotation));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            // NotNull on child join keys means LEFT JOIN can be upgraded to INNER JOIN
            assertThat(sql, not(containsString("left outer join")));
            assertThat(sql, containsString("join ("));
            assertThat(sql, containsString("\"parent_0\""));
        }

        @Test
        void compile_noNotNullOnChildJoinKeys_keepsLeftJoin() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            // No NotNull annotations at all
            var view = createJsonViewWithJoinsAndAnnotations(
                    "employees.json",
                    null,
                    Set.of(expressionField("emp_name", "name")),
                    Set.of(viewJoin),
                    Set.of(),
                    Set.of());
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            // Without NotNull, LEFT JOIN stays as LEFT JOIN
            assertThat(sql, containsString("left outer join"));
        }

        @Test
        void compile_primaryKeyNotInProjection_retainsDistinct() {
            var idField = expressionField("id", "id");
            var nameField = expressionField("name", "name");

            // PK on {id, name}, but only "name" is projected — PK not fully covered
            var pkAnnotation = primaryKeyAnnotation(idField, nameField);
            var view =
                    createJsonViewWithAnnotations("data.json", null, Set.of(idField, nameField), Set.of(pkAnnotation));
            var context = EvaluationContext.of(Set.of("name"), DedupStrategy.exact(), null);

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("select distinct"));
        }

        @Test
        void compile_foreignKeyWithEmptyProjection_retainsJoin() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var childFkField = expressionField("dept_id", "dept_id");
            var parentFkField = expressionField("dept_id", "id");
            var fkAnnotation = foreignKeyAnnotation(parentView, List.of(childFkField), List.of(parentFkField));

            // Empty projection = "all fields" — parent fields ARE implicitly projected
            var view = createJsonViewWithJoinsAndAnnotations(
                    "employees.json",
                    null,
                    Set.of(expressionField("emp_name", "name")),
                    Set.of(viewJoin),
                    Set.of(),
                    Set.of(fkAnnotation));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("left outer join"));
            assertThat(sql, containsString("\"parent_0\""));
        }

        @Test
        void compile_foreignKeyTargetingDifferentParent_retainsJoin() {
            var actualParent = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));
            var differentParent = createJsonView("offices.json", null, Set.of(expressionField("office_id", "id")));

            var joinField = expressionField("department_name", "dept_name");
            var joinCond = joinCondition("dept_id", "dept_id");
            var viewJoin = logicalViewJoin(actualParent, Set.of(joinCond), Set.of(joinField));

            // FK points to differentParent, NOT actualParent
            var childFkField = expressionField("office_id", "office_id");
            var parentFkField = expressionField("office_id", "id");
            var fkAnnotation = foreignKeyAnnotation(differentParent, List.of(childFkField), List.of(parentFkField));

            var view = createJsonViewWithJoinsAndAnnotations(
                    "employees.json",
                    null,
                    Set.of(expressionField("emp_name", "name")),
                    Set.of(viewJoin),
                    Set.of(),
                    Set.of(fkAnnotation));
            var context = EvaluationContext.withProjectedFields(Set.of("emp_name"));

            var sql = DuckDbViewCompiler.compile(view, context);

            // FK does not match this join's parent — join must be retained
            assertThat(sql, containsString("left outer join"));
            assertThat(sql, containsString("\"parent_0\""));
        }

        @Test
        void compile_notNullOnPartialChildJoinKeys_keepsLeftJoin() {
            var parentView = createJsonView(
                    "details.json", null, Set.of(expressionField("id", "id"), expressionField("region", "region")));

            var joinField = expressionField("detail_region", "region");
            var cond1 = joinCondition("id", "id");
            var cond2 = joinCondition("region", "region");
            var viewJoin = logicalViewJoin(parentView, Set.of(cond1, cond2), Set.of(joinField));

            // NotNull only covers "id", not "region"
            var idField = expressionField("id", "id");
            var notNullAnnotation = notNullAnnotation(idField);

            var view = createJsonViewWithJoinsAndAnnotations(
                    "main.json",
                    null,
                    Set.of(expressionField("name", "name")),
                    Set.of(viewJoin),
                    Set.of(),
                    Set.of(notNullAnnotation));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            // "region" is not NotNull — LEFT JOIN must be retained
            assertThat(sql, containsString("left outer join"));
        }
    }
}
