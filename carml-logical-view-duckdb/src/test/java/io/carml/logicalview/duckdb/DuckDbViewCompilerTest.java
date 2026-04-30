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
import java.util.function.UnaryOperator;
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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("read_text('data.json')"));
            assertThat(sql, containsString("json_extract(content, '$.items[*]')"));
            assertThat(sql, containsString("unnest("));
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", 'id') \"id\""));
        }

        @Test
        void compile_jsonSourceWithIterator_producesTypeCompanionColumn() {
            var view = createJsonView("data.json", "$.items[*]", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Type companion column should use json_type from the iterator column
            assertThat(sql, containsString("json_type(\"view_source\".\"__iter\", 'id') \"id.__type\""));
        }

        @Test
        void compile_jsonSourceWithoutIterator_producesCastNullTypeCompanion() {
            var view = createJsonView("data.json", null, Set.of(expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Without iterator, read_json_auto is used (column strategy) => CAST(NULL AS VARCHAR)
            assertThat(sql, containsString("cast(null as varchar) \"name.__type\""));
            assertThat(sql, not(containsString("json_type")));
        }

        @Test
        void compile_jsonSourceWithBlankIterator_omitsJsonPath() {
            var view = createJsonView("data.json", "   ", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("read_json_auto('data.json')"));
            assertThat(sql, not(containsString("json_path")));
        }

        @Test
        void compile_jsonSourceWithoutIterator_omitsJsonPath() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("read_text('data.json')"));
            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("json_extract_string(\"__iter\", '$.name') = 'alice'"));
        }

        @Test
        void compile_jsonIteratorWithGreaterThanFilter_producesWhereClause() {
            var view =
                    createJsonView("data.json", "$.items[?(@.price > 10)]", Set.of(expressionField("price", "price")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("cast(json_extract_string(\"__iter\", '$.price') as double) > 1E1"));
        }

        @Test
        void compile_jsonIteratorWithExistsFilter_producesIsNotNullClause() {
            var view = createJsonView("data.json", "$.items[?(@.type)]", Set.of(expressionField("type", "type")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("json_extract(\"__iter\", '$.type') is not null"));
        }

        @Test
        void compile_jsonIteratorWithRegexFilter_producesRegexpMatches() {
            var view = createJsonView(
                    "data.json", "$.items[?(@.name =~ /^test/)]", Set.of(expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("regexp_matches(json_extract_string(\"__iter\", '$.name'), '^test')"));
        }

        @Test
        void compile_jsonIteratorWithEqualNumFilter_producesWhereClause() {
            var view =
                    createJsonView("data.json", "$.items[?(@.count == 5)]", Set.of(expressionField("count", "count")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("cast(json_extract_string(\"__iter\", '$.count') as double) = 5E0"));
        }

        @Test
        void compile_jsonIteratorWithEqualBoolFilter_producesWhereClause() {
            var view = createJsonView(
                    "data.json", "$.items[?(@.active == true)]", Set.of(expressionField("active", "active")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("cast(json_extract_string(\"__iter\", '$.active') as boolean) = true"));
        }

        @Test
        void compile_jsonIteratorWithLessThanFilter_producesWhereClause() {
            var view =
                    createJsonView("data.json", "$.items[?(@.price < 20)]", Set.of(expressionField("price", "price")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("where"));
            assertThat(sql, containsString("cast(json_extract_string(\"__iter\", '$.price') as double) < 2E1"));
        }
    }

    // --- CSV source tests ---

    @Nested
    class CsvSource {

        @Test
        void compile_csvSource_producesReadCsvAllVarchar() {
            var view = createCsvView("data.csv", Set.of(expressionField("col1", "column1")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("read_csv('data.csv'"));
            assertThat(sql, containsString("all_varchar = true"));
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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("read_parquet('data.parquet')"));
            assertThat(sql, not(containsString("read_json_auto")));
        }

        @Test
        void compile_csvSourceWithParquetExtension_producesReadParquet() {
            var view = createCsvView("data.parq", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("read_parquet('data.parq')"));
            assertThat(sql, not(containsString("read_csv(")));
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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("(SELECT id, name FROM users)"));
        }

        @Test
        void compile_sqlSourceWithTableName_producesTableReference() {
            var view = createSqlViewWithTable("users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("from \"users\""));
        }

        @Test
        void compile_sqlSourceWithDatabaseSourceFallback_producesSubquery() {
            var dbSource = mock(DatabaseSource.class);
            when(dbSource.getQuery()).thenReturn("SELECT * FROM orders");

            var view = createSqlViewWithDatabaseSource(dbSource, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("(SELECT * FROM orders)"));
        }

        @Test
        void compile_sqlSource_producesTypeofTypeCompanion() {
            var view = createSqlViewWithQuery(
                    "SELECT id, name FROM users", Set.of(expressionField("id", "id"), expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // SQL sources should use typeof() for type companions, not CAST(NULL AS VARCHAR)
            assertThat(sql, containsString("typeof(\"view_source\".\"id\") \"id.__type\""));
            assertThat(sql, containsString("typeof(\"view_source\".\"name\") \"name.__type\""));
            assertThat(sql, not(containsString("cast(null as varchar)")));
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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("\"name\" \"name\""));
            assertThat(sql, not(containsString("\"age\" \"age\"")));
            assertThat(sql, not(containsString("\"email\" \"email\"")));
        }

        @Test
        void compile_withEmptyProjectedFields_selectsAllFields() {
            var view = createJsonView(
                    "data.json", null, Set.of(expressionField("name", "name"), expressionField("age", "age")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, not(containsString("select distinct")));
            assertThat(sql, not(containsString("\"deduped\"")));
        }

        @Test
        void compile_withExactDedupStrategy_usesDistinctCte() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("limit 100"));
        }

        @Test
        void compile_withoutLimit_omitsLimitClause() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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
            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("'fixed_value' \"status\""));
        }
    }

    // --- Error handling tests ---

    @Nested
    class ErrorHandling {

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("\"my\"\"column\" \"col\""));
        }

        @Test
        void compile_filePathWithSingleQuote_escapesSingleQuotes() {
            var view = createJsonView("data's.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("read_json_auto('data''s.json')"));
        }

        @Test
        void compile_fieldNameWithSpecialChars_quotesAlias() {
            var view = createJsonView("data.json", null, Set.of(expressionField("my-field", "ref")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("\"ref\" \"my-field\""));
        }

        @Test
        void compile_fieldNameWithAlphanumericChars_unquotedAlias() {
            var view = createJsonView("data.json", null, Set.of(expressionField("myField_1", "ref")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("read_json_auto('data.json')"));
        }

        @Test
        void compile_rmlSQL2008TableFormulation_producesSqlSource() {
            var view = createSqlViewWithQueryAndFormulation(
                    Rdf.Rml.SQL2008Table, null, "users", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("from \"users\""));
        }

        @Test
        void compile_rmlSQL2008QueryFormulation_producesSqlSource() {
            var view = createSqlViewWithQueryAndFormulation(
                    Rdf.Rml.SQL2008Query, "SELECT * FROM people", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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
        void compile_rmlCsvFormulation_producesReadCsvAllVarchar() {
            var view = createViewWithRefFormulation(Rdf.Rml.Csv, "data.csv", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("read_csv('data.csv'"));
            assertThat(sql, containsString("all_varchar = true"));
        }
    }

    // --- SQL structure verification ---

    @Nested
    class SqlStructure {

        @Test
        void compile_producesWellFormedCte() {
            var view = createJsonView("people.json", null, Set.of(expressionField("name", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            var expected = "with \"view_source\" as ("
                    + "select *, row_number() over () \"__idx\" from read_json_auto('people.json')"
                    + ") select \"view_source\".\"name\" \"name\", "
                    + "cast(0 as bigint) \"name.#\", "
                    + "cast(null as varchar) \"name.__type\", "
                    + "\"view_source\".\"__idx\" "
                    + "from \"view_source\"";

            assertThat(sql, is(expected));
        }

        @Test
        void compile_withDistinctAndLimit_producesCorrectStructure() {
            var view = createJsonView("people.json", null, Set.of(expressionField("name", "name")));
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), 10L);

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            var expected = "with \"view_source\" as ("
                    + "select *, row_number() over () \"__idx\" from read_json_auto('people.json')"
                    + "), \"deduped\" as ("
                    + "select distinct \"view_source\".\"name\" \"name\", "
                    + "cast(0 as bigint) \"name.#\", "
                    + "cast(null as varchar) \"name.__type\" "
                    + "from \"view_source\""
                    + ") select *, "
                    + "row_number() over () \"__idx\" "
                    + "from \"deduped\" "
                    + "limit 10";

            assertThat(sql, is(expected));
        }
    }

    // --- Multi-valued ExpressionField compilation tests ---

    @Nested
    class MultiValuedExpressionFieldCompilation {

        @Test
        void compile_multiValuedExpressionField_producesUnnestExpansion() {
            var view = createJsonView("data.json", "$.people[*]", Set.of(expressionField("item", "$.items[*]")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Multi-valued field should use LATERAL UNNEST
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(json_extract(\"view_source\".\"__iter\", '$.items[*]'))"));
            // Value extraction from unnested element
            assertThat(sql, containsString("json_extract_string(\"item\".\"unnest\", '$') \"item\""));
            // Ordinal column
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
        }

        @Test
        void compile_singleValuedExpressionField_producesOrdinalCompanion() {
            var view = createJsonView("data.json", "$.people[*]", Set.of(expressionField("name", "$.name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Single-valued field should have ordinal companion of 0 (BIGINT)
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.name') \"name\""));
            assertThat(sql, containsString("cast(0 as bigint) \"name.#\""));
        }

        @Test
        void compile_mixedSingleAndMultiValuedFields_producesCorrectSql() {
            var nameField = expressionField("name", "$.name");
            var itemField = expressionField("item", "$.items[*]");

            var fields = new LinkedHashSet<Field>();
            fields.add(nameField);
            fields.add(itemField);

            var view = createJsonViewWithFields("data.json", "$.people[*]", fields);
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Single-valued field with ordinal companion (BIGINT)
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.name') \"name\""));
            assertThat(sql, containsString("cast(0 as bigint) \"name.#\""));
            // Multi-valued field with UNNEST
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("json_extract_string(\"item\".\"unnest\", '$') \"item\""));
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
        }

        @Test
        void compile_joinFieldProjectsOrdinal() {
            var parentView = createJsonView(
                    "items.json", null, Set.of(expressionField("item_id", "id"), expressionField("value", "value")));

            var joinField = expressionField("parent_value", "value");
            var joinCond = joinCondition("item_id", "item_id");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "main.json", null, Set.of(expressionField("name", "name")), Set.of(viewJoin), Set.of());
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Join projected field
            assertThat(sql, containsString("\"parent_0\".\"value\" \"parent_value\""));
            // Join projected field ordinal
            assertThat(sql, containsString("\"parent_0\".\"value.#\" \"parent_value.#\""));
            // Join projected field type companion
            assertThat(sql, containsString("\"parent_0\".\"value.__type\" \"parent_value.__type\""));
        }

        @Test
        void compile_multiValuedExpressionFieldWithFilter_producesFilteredUnnest() {
            var view = createJsonView(
                    "data.json", "$.people[*]", Set.of(expressionField("item", "$.items[?(@.active==true)]")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // The UNNEST should use the normalized basePath ($.items[*]), not the raw filter path
            assertThat(sql, containsString("unnest(json_extract(\"view_source\".\"__iter\", '$.items[*]'))"));
            // A WHERE clause should apply the filter condition on the "unnest" column
            assertThat(sql, containsString("WHERE"));
            assertThat(sql, containsString("cast(json_extract_string(\"unnest\", '$.active') as boolean) = true"));
            // Ordinals should be recomputed after filtering
            assertThat(sql, containsString("row_number() over()"));
            // Value extraction and ordinal projection should still work
            assertThat(sql, containsString("json_extract_string(\"item\".\"unnest\", '$') \"item\""));
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
        }

        @Test
        void compile_multiValuedExpressionField_wrapsLateralWithEmptyPreservingFallback() {
            // When the multi-valued reference resolves to an empty array for a given parent row,
            // the comma-cross-joined LATERAL would otherwise drop the parent row entirely. The
            // compiler emits a UNION ALL fallback that injects a single (NULL, 0) sentinel row
            // when NOT EXISTS detects an empty inner result, preserving the parent row so
            // triples that depend only on other fields (or on join projections) survive.
            var view = createJsonView("data.json", "$.people[*]", Set.of(expressionField("item", "$.items[*]")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Standard array unnest body is still emitted (unchanged correlation to view_source).
            assertThat(sql, containsString("unnest(json_extract(\"view_source\".\"__iter\", '$.items[*]'))"));
            // Empty-preservation fallback: UNION ALL emits a NULL sentinel row, gated by NOT
            // EXISTS so it only fires when the inner unnest produces zero rows.
            assertThat(sql, containsString("UNION ALL"));
            assertThat(sql, containsString("SELECT NULL AS \"unnest\", 0 AS \"__ord\""));
            assertThat(sql, containsString("WHERE NOT EXISTS"));
            assertThat(sql, containsString("\"_empty_check\""));
        }

        @Test
        void compile_multiValuedExpressionFieldWithFilter_wrapsLateralWithEmptyPreservingFallback() {
            // Filter expressions can produce zero surviving rows even from a non-empty parent
            // array; the empty-preserving wrap covers this case too.
            var view = createJsonView(
                    "data.json", "$.people[*]", Set.of(expressionField("item", "$.items[?(@.active==true)]")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("UNION ALL"));
            assertThat(sql, containsString("SELECT NULL AS \"unnest\", 0 AS \"__ord\""));
            assertThat(sql, containsString("WHERE NOT EXISTS"));
            assertThat(sql, containsString("\"_empty_check\""));
        }

        @Test
        void compile_multiValuedExpressionFieldWithBracketKey_skipsEmptyPreservingFallback() {
            // JSON bracket-quoted single-key references (e.g., $['Country Code']) are analysed
            // as a name-union with one name. Because name unions emit a fixed positive number
            // of rows per parent, the inner LATERAL never produces zero rows and the
            // empty-preservation fallback would only introduce optimizer-driven row reordering
            // without changing semantics. The compiler therefore skips the wrap.
            var view =
                    createJsonView("data.json", "$.countries[*]", Set.of(expressionField("code", "$['Country Code']")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // The plain LATERAL with list_value(...) emission is preserved unchanged — no
            // UNION ALL fallback is added for this always-non-empty path.
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, not(containsString("\"_empty_check\"")));
        }
    }

    // --- IterableField / UNNEST compilation tests ---

    @Nested
    class IterableFieldCompilation {

        @Test
        void compile_iterableFieldWithFilteredIterator_passesFilterToDuckDb() {
            var nestedField = expressionField("item_name", "$.name");
            var iterableField = iterableField("item", "$.items[?(@.active==true)]", Set.of(nestedField));

            var view = createJsonViewWithFields("data.json", "$.people[*]", Set.of(iterableField));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // The raw JSONPath with filter should be passed through to DuckDB
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("json_extract(\"view_source\".\"__iter\", '$.items[?(@.active==true)]')"));
            // Nested field extraction and ordinal
            assertThat(sql, containsString("json_extract_string(\"item\".\"unnest\", '$.name') \"item.item_name\""));
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
        }

        @Test
        void compile_iterableFieldWithNestedExpressionFields_producesUnnest() {
            var nestedField = expressionField("item_type", "type");
            var iterableField = iterableField("item", "items", Set.of(nestedField));

            var view = createJsonViewWithFields("data.json", null, Set.of(iterableField));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("\"deduped\" as ("));
            assertThat(sql, containsString("select distinct"));
            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(\"view_source\".\"items\")"));
            assertThat(sql, containsString("\"item\".\"type\" \"item.item_type\""));
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
        }

        @Test
        void compile_jsonIteratorWithIterableField_producesNestedTypeCompanion() {
            var nestedField = expressionField("contact_phone", "$.phone");
            var iterable = iterableField("contacts", "$.contacts[*]", Set.of(nestedField));

            var fields = new LinkedHashSet<Field>();
            fields.add(expressionField("name", "$.name"));
            fields.add(iterable);

            var view = createJsonViewWithFields("data.json", "$.people[*]", fields);
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Top-level field type companion uses json_type from __iter
            assertThat(sql, containsString("json_type(\"view_source\".\"__iter\", '$.name') \"name.__type\""));
            // Nested iterable field type companion uses json_type from unnest
            assertThat(
                    sql,
                    containsString("json_type(\"contacts\".\"unnest\", '$.phone') \"contacts.contact_phone.__type\""));
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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, containsString("LATERAL"));
            assertThat(sql, containsString("unnest(\"view_source\".\"items\")"));
            assertThat(sql, containsString("left outer join"));
            assertThat(sql, containsString("\"item\".\"type\" \"item.item_type\""));
            assertThat(sql, containsString("\"item\".\"__ord\" \"item.#\""));
            assertThat(sql, containsString("\"parent_0\".\"dept_name\" \"department_name\""));
        }

        @Test
        void compile_joinConditionParentTemplateChildReference_producesConcatOnParentSide() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var join = joinConditionWithMaps(
                    referenceMap(ChildMap.class, "dept_id"),
                    templateMap(
                            ParentMap.class,
                            new CarmlTemplate.TextSegment("dept-"),
                            new CarmlTemplate.ExpressionSegment(0, "dept_id")));
            var viewJoin = logicalViewJoin(parentView, Set.of(join), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "employees.json",
                    "$.employees[*]",
                    Set.of(expressionField("dept_id", "$.dept_id")),
                    Set.of(viewJoin),
                    Set.of());

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.dept_id')"));
            assertThat(sql, containsString("'dept-'"));
            assertThat(sql, containsString("\"parent_0\".\"dept_id\""));
            assertThat(sql, containsString("||"));
        }

        @Test
        void compile_joinConditionParentReferenceChildTemplate_producesConcatOnChildSide() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var join = joinConditionWithMaps(
                    templateMap(
                            ChildMap.class,
                            new CarmlTemplate.TextSegment("dept-"),
                            new CarmlTemplate.ExpressionSegment(0, "$.dept_id")),
                    referenceMap(ParentMap.class, "dept_id"));
            var viewJoin = logicalViewJoin(parentView, Set.of(join), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "employees.json",
                    "$.employees[*]",
                    Set.of(expressionField("dept_id", "$.dept_id")),
                    Set.of(viewJoin),
                    Set.of());

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            assertThat(sql, containsString("'dept-'"));
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.dept_id')"));
            assertThat(sql, containsString("\"parent_0\".\"dept_id\""));
            assertThat(sql, containsString("||"));
        }

        @Test
        void compile_joinConditionParentConstantChildReference_producesStringLiteralOnParentSide() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var join = joinConditionWithMaps(
                    referenceMap(ChildMap.class, "$.kind"), constantMap(ParentMap.class, "human"));
            var viewJoin = logicalViewJoin(parentView, Set.of(join), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "employees.json",
                    "$.employees[*]",
                    Set.of(expressionField("kind", "$.kind")),
                    Set.of(viewJoin),
                    Set.of());

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.kind')"));
            assertThat(sql, containsString("'human'"));
        }

        @Test
        void compile_joinConditionParentReferenceChildConstant_producesStringLiteralOnChildSide() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var join =
                    joinConditionWithMaps(constantMap(ChildMap.class, "human"), referenceMap(ParentMap.class, "kind"));
            var viewJoin = logicalViewJoin(parentView, Set.of(join), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "employees.json",
                    "$.employees[*]",
                    Set.of(expressionField("name", "$.name")),
                    Set.of(viewJoin),
                    Set.of());

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            assertThat(sql, containsString("'human'"));
            assertThat(sql, containsString("\"parent_0\".\"kind\""));
        }

        @Test
        void compile_joinConditionParentTemplateChildConstant_producesNonTrivialBothSides() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var join = joinConditionWithMaps(
                    constantMap(ChildMap.class, "fixed"),
                    templateMap(
                            ParentMap.class,
                            new CarmlTemplate.TextSegment("X-"),
                            new CarmlTemplate.ExpressionSegment(0, "dept_id")));
            var viewJoin = logicalViewJoin(parentView, Set.of(join), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "employees.json",
                    "$.employees[*]",
                    Set.of(expressionField("name", "$.name")),
                    Set.of(viewJoin),
                    Set.of());

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            assertThat(sql, containsString("'fixed'"));
            assertThat(sql, containsString("'X-'"));
            assertThat(sql, containsString("\"parent_0\".\"dept_id\""));
        }

        @Test
        void compile_joinConditionParentConstantChildTemplate_producesNonTrivialBothSides() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var join = joinConditionWithMaps(
                    templateMap(
                            ChildMap.class,
                            new CarmlTemplate.TextSegment("X-"),
                            new CarmlTemplate.ExpressionSegment(0, "$.dept_id")),
                    constantMap(ParentMap.class, "fixed"));
            var viewJoin = logicalViewJoin(parentView, Set.of(join), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "employees.json",
                    "$.employees[*]",
                    Set.of(expressionField("dept_id", "$.dept_id")),
                    Set.of(viewJoin),
                    Set.of());

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            assertThat(sql, containsString("'X-'"));
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.dept_id')"));
            assertThat(sql, containsString("'fixed'"));
        }

        @Test
        void compile_joinConditionConstantParentColumnSourceStrategy_producesStringLiteralOnParentSide() {
            // ColumnSourceStrategy is used for view-on-view sources; here we exercise it by
            // creating a CSV view (which uses ColumnSourceStrategy) and asserting the constant
            // parent side is emitted as a SQL string literal.
            var parentView = createCsvView(
                    "departments.csv", Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var join =
                    joinConditionWithMaps(referenceMap(ChildMap.class, "kind"), constantMap(ParentMap.class, "human"));
            var viewJoin = logicalViewJoin(parentView, Set.of(join), Set.of(joinField));

            var view = createCsvViewWithJoins(
                    "employees.csv", Set.of(expressionField("kind", "kind")), Set.of(viewJoin), Set.of());

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            assertThat(sql, containsString("\"view_source\".\"kind\""));
            assertThat(sql, containsString("'human'"));
        }

        @Test
        void compile_joinConditionParentTemplateChildTemplate_producesConcatOnBothSides() {
            var parentView = createJsonView(
                    "departments.json",
                    null,
                    Set.of(expressionField("dept_id", "id"), expressionField("dept_name", "name")));

            var joinField = expressionField("department_name", "dept_name");
            var join = joinConditionWithMaps(
                    templateMap(
                            ChildMap.class,
                            new CarmlTemplate.TextSegment("dept-"),
                            new CarmlTemplate.ExpressionSegment(0, "$.dept_id")),
                    templateMap(
                            ParentMap.class,
                            new CarmlTemplate.TextSegment("dept-"),
                            new CarmlTemplate.ExpressionSegment(0, "dept_id")));
            var viewJoin = logicalViewJoin(parentView, Set.of(join), Set.of(joinField));

            var view = createJsonViewWithJoins(
                    "employees.json",
                    "$.employees[*]",
                    Set.of(expressionField("dept_id", "$.dept_id")),
                    Set.of(viewJoin),
                    Set.of());

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            assertThat(sql, containsString("'dept-'"));
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.dept_id')"));
            assertThat(sql, containsString("\"parent_0\".\"dept_id\""));
            assertThat(sql, containsString("||"));
        }

        @Test
        void compile_joinProjectedFieldTemplate_producesConcatExpression() {
            // Mirrors RMLLVTC0010d: a leftJoin projects a parent field whose value is
            // a template referencing two parent fieldNames (json_first_name, json_last_name).
            var parentView = createJsonView(
                    "people.json",
                    "$.people[*]",
                    Set.of(
                            expressionField("json_first_name", "$.firstName"),
                            expressionField("json_last_name", "$.lastName")));

            var joinField = templateExpressionField(
                    "json_full_name",
                    new CarmlTemplate.ExpressionSegment(0, "json_first_name"),
                    new CarmlTemplate.TextSegment(" "),
                    new CarmlTemplate.ExpressionSegment(1, "json_last_name"));

            var joinCond = joinCondition("csv_name", "json_first_name");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var view = createCsvViewWithJoins(
                    "people.csv", Set.of(expressionField("csv_name", "name")), Set.of(viewJoin), Set.of());

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            // Template variables resolve to parent-alias-qualified columns of the compiled parent
            // subquery. The parent view exposes each fieldName as a column.
            assertThat(sql, containsString("\"parent_0\".\"json_first_name\""));
            assertThat(sql, containsString("\"parent_0\".\"json_last_name\""));
            // Literal segment between the two variables.
            assertThat(sql, containsString("' '"));
            // DuckDB uses || for string concatenation.
            assertThat(sql, containsString("||"));
            // The whole concatenation is aliased to the projection field name.
            assertThat(sql, containsString("\"json_full_name\""));
            // No reference-style ordinal/type companion columns are emitted for template projections.
            assertThat(sql, not(containsString("\"json_full_name.#\"")));
            assertThat(sql, not(containsString("\"json_full_name.__type\"")));
        }

        @Test
        void compile_joinProjectedFieldConstant_producesInlineLiteral() {
            // Mirrors RMLLVTC0010e: an innerJoin projects a constant-valued field "status".
            var parentView =
                    createJsonView("people.json", "$.people[*]", Set.of(expressionField("json_name", "$.name")));

            var joinField = constantExpressionField("status", "student");
            var joinCond = joinCondition("csv_name", "json_name");
            var viewJoin = logicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));

            var view = createCsvViewWithJoins(
                    "people.csv", Set.of(expressionField("csv_name", "name")), Set.of(), Set.of(viewJoin));

            var sql = DuckDbViewCompiler.compile(view, EvaluationContext.defaults())
                    .sql();

            // The constant becomes a SQL string literal aliased to the projection field name.
            assertThat(sql, containsString("'student'"));
            assertThat(sql, containsString("\"status\""));
            // INNER JOIN, not LEFT.
            assertThat(sql, not(containsString("left outer join")));
            // No reference-style ordinal/type companion columns are emitted for constant projections.
            assertThat(sql, not(containsString("\"status.#\"")));
            assertThat(sql, not(containsString("\"status.__type\"")));
        }
    }

    // --- Mixed-formulation iterable field tests ---

    @Nested
    class MixedFormulationIterable {

        @Test
        void compile_csvParentWithJsonArrayChild_producesJsonExtractUnnest() {
            // CSV source with a column "items" containing JSON array text.
            // The IterableField child uses JSONPath formulation with array iterator $[*].
            var nestedType = expressionField("type", "$.type");
            var nestedWeight = expressionField("weight", "$.weight");
            var jsonIterable =
                    mixedFormulationIterableField("item", "$[*]", Rdf.Ql.JsonPath, Set.of(nestedType, nestedWeight));

            var parentField = expressionFieldWithChildren("items", "items", Set.of(jsonIterable));
            var idField = expressionField("id", "id");

            var fields = new LinkedHashSet<Field>();
            fields.add(idField);
            fields.add(parentField);

            var view = createCsvViewWithMixedFields("data.csv", fields);
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // CSV source read
            assertThat(sql, containsString("read_csv('data.csv'"));
            assertThat(sql, containsString("all_varchar = true"));
            // Parent CSV column projected
            assertThat(sql, containsString("\"items\" \"items\""));
            // JSON array extraction and unnest from parent column
            assertThat(sql, containsString("json_extract(\"view_source\".\"items\""));
            assertThat(sql, containsString("$[*]"));
            // Ordinal generation via range(len(...))
            assertThat(sql, containsString("unnest(range(len("));
            // Nested field extraction using json_extract_string from unnested JSON
            assertThat(sql, containsString("json_extract_string("));
            assertThat(sql, containsString("'$.type'"));
            assertThat(sql, containsString("\"items.item.type\""));
            assertThat(sql, containsString("'$.weight'"));
            assertThat(sql, containsString("\"items.item.weight\""));
            // Type companion columns via json_type
            assertThat(sql, containsString("json_type("));
            assertThat(sql, containsString("\"items.item.type.__type\""));
            assertThat(sql, containsString("\"items.item.weight.__type\""));
            // Ordinal column
            assertThat(sql, containsString("\"items.item.#\""));
        }

        @Test
        void compile_csvParentWithJsonSingleValueChild_producesListValueWrapping() {
            // CSV source with a column "metadata" containing a single JSON object.
            // The IterableField child uses JSONPath formulation with single-value iterator $.
            var nestedName = expressionField("name", "$.name");
            var jsonIterable = mixedFormulationIterableField("entry", "$", Rdf.Ql.JsonPath, Set.of(nestedName));

            var parentField = expressionFieldWithChildren("metadata", "metadata", Set.of(jsonIterable));

            var fields = new LinkedHashSet<Field>();
            fields.add(parentField);

            var view = createCsvViewWithMixedFields("data.csv", fields);
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Single-value iterator wraps in list_value for a single-row unnest
            assertThat(sql, containsString("list_value(json_extract("));
            assertThat(sql, containsString("\"view_source\".\"metadata\""));
            // Nested field extraction
            assertThat(sql, containsString("json_extract_string("));
            assertThat(sql, containsString("'$.name'"));
            assertThat(sql, containsString("\"metadata.entry.name\""));
        }

        @Test
        void compile_jsonParentWithCsvChild_producesStringSplitUnnest() {
            // JSON source with iterator, containing a field "csv_data" with embedded CSV text.
            // The IterableField child uses CSV formulation.
            var nestedCity = expressionField("city", "city");
            var nestedPop = expressionField("population", "population");
            var csvIterable = mixedFormulationIterableField("row", null, Rdf.Ql.Csv, Set.of(nestedCity, nestedPop));

            var parentField = expressionFieldWithChildren("csv_data", "$.csv_data", Set.of(csvIterable));
            var nameField = expressionField("name", "$.name");

            var fields = new LinkedHashSet<Field>();
            fields.add(nameField);
            fields.add(parentField);

            var view = createJsonViewWithFields("data.json", "$.records[*]", fields);
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // JSON source with iterator
            assertThat(sql, containsString("read_text('data.json')"));
            assertThat(sql, containsString("json_extract(content, '$.records[*]')"));
            // Top-level JSON field
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.name') \"name\""));
            // CSV row splitting: string_split by newline, skip header with [2:]
            assertThat(sql, containsString("string_split("));
            assertThat(sql, containsString("chr(10)"));
            assertThat(sql, containsString("[2:]"));
            // Column position lookup via list_position
            assertThat(sql, containsString("list_position("));
            // Field extraction via list_extract
            assertThat(sql, containsString("list_extract("));
            assertThat(sql, containsString("\"csv_data.row.city\""));
            assertThat(sql, containsString("\"csv_data.row.population\""));
            // Ordinal column
            assertThat(sql, containsString("\"csv_data.row.#\""));
            // CSV type companions are null (all strings)
            assertThat(sql, containsString("\"csv_data.row.city.__type\""));
            assertThat(sql, containsString("\"csv_data.row.population.__type\""));
        }

        @Test
        void compile_csvParentWithJsonChildAndProjection_selectsOnlyProjectedNestedFields() {
            // CSV source with a column "items" containing JSON array text.
            // Only "items.item.type" is projected, so "items.item.weight" should be excluded.
            var nestedType = expressionField("type", "$.type");
            var nestedWeight = expressionField("weight", "$.weight");
            var jsonIterable =
                    mixedFormulationIterableField("item", "$[*]", Rdf.Ql.JsonPath, Set.of(nestedType, nestedWeight));

            var parentField = expressionFieldWithChildren("items", "items", Set.of(jsonIterable));

            var fields = new LinkedHashSet<Field>();
            fields.add(parentField);

            var view = createCsvViewWithMixedFields("data.csv", fields);
            var context = EvaluationContext.withProjectedFields(Set.of("items.item.type"));

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Projected field IS present
            assertThat(sql, containsString("\"items.item.type\""));
            // Non-projected field is NOT present
            assertThat(sql, not(containsString("\"items.item.weight\"")));
            // Parent expression field is still included (needed for mixed-formulation child)
            assertThat(sql, containsString("\"items\""));
            // Ordinal column is present
            assertThat(sql, containsString("\"items.item.#\""));
        }
    }

    // --- Source table resolver tests ---

    @Nested
    class SourceTableResolver {

        @Test
        void compile_withResolver_substitutesTableNameInSql() {
            var view = createJsonView("file.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            // The resolver returns a fully qualified table reference (as produced by
            // DuckDbSourceTableCache.qualify()), which the compiler uses as-is.
            var compiledView =
                    DuckDbViewCompiler.compile(view, context, sourceSql -> "\"memory\".\"main\".\"__carml_src_0\"");

            assertThat(compiledView.sql(), containsString("\"__carml_src_0\""));
            assertThat(compiledView.sql(), not(containsString("read_")));
        }

        @Test
        void compile_withResolver_returningNull_usesRawSourceSql() {
            var view = createJsonView("file.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var compiledView = DuckDbViewCompiler.compile(view, context, sourceSql -> null);

            assertThat(compiledView.sql(), not(containsString("__carml_src")));
            assertThat(compiledView.sql(), not(containsString("\"null\"")));
        }

        @Test
        void compile_withNullResolver_producesSameSqlAsTwoArgCompile() {
            var view = createJsonView("file.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var withNull = DuckDbViewCompiler.compile(view, context, null);
            var without = DuckDbViewCompiler.compile(view, context);

            assertThat(withNull.sql(), is(without.sql()));
        }

        @Test
        void compile_withResolver_exceptionInCompile_cleansUpThreadLocal() {
            var view = mock(LogicalView.class);
            lenient().when(view.getViewOn()).thenReturn(mock(AbstractLogicalSource.class));
            lenient().when(view.getFields()).thenReturn(Set.of());
            lenient().when(view.getLeftJoins()).thenReturn(Set.of());
            lenient().when(view.getInnerJoins()).thenReturn(Set.of());
            lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());
            lenient().when(view.getResourceName()).thenReturn("failView");

            UnaryOperator<String> resolver = sql -> "__carml_src_0";
            var context = EvaluationContext.defaults();
            assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view, context, resolver));

            // After the exception the ThreadLocal must be clean — a subsequent compile
            // must not see a stale resolver.
            var goodView = createJsonView("good.json", null, Set.of(expressionField("id", "id")));
            var sql = DuckDbViewCompiler.compile(goodView, EvaluationContext.defaults())
                    .sql();
            assertThat(sql, not(containsString("__carml_src")));
        }
    }

    // --- Helper methods ---

    private static ExpressionField expressionField(String fieldName, String reference) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(reference);
        return field;
    }

    private static ExpressionField templateExpressionField(String fieldName, CarmlTemplate.Segment... segments) {
        var template = mock(Template.class);
        lenient().when(template.getSegments()).thenReturn(List.of(segments));

        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(null);
        lenient().when(field.getTemplate()).thenReturn(template);
        return field;
    }

    private static ExpressionField constantExpressionField(String fieldName, String constantValue) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(null);
        lenient()
                .when(field.getConstant())
                .thenReturn(SimpleValueFactory.getInstance().createLiteral(constantValue));
        return field;
    }

    /**
     * Creates an ExpressionField mock that has child fields, used for mixed-formulation scenarios
     * where a parent expression field contains embedded data in a different format.
     */
    @SuppressWarnings("unchecked")
    private static ExpressionField expressionFieldWithChildren(
            String fieldName, String reference, Set<? extends Field> children) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(reference);
        lenient().when(field.getFields()).thenReturn((Set<Field>) children);
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

    /**
     * Creates an IterableField mock with a reference formulation, used for mixed-formulation
     * scenarios where a child iterable uses a different formulation than its parent source.
     */
    @SuppressWarnings("unchecked")
    private static IterableField mixedFormulationIterableField(
            String fieldName,
            String iterator,
            org.eclipse.rdf4j.model.Resource formulationIri,
            Set<ExpressionField> nestedFields) {
        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(formulationIri);

        var field = mock(IterableField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getIterator()).thenReturn(iterator);
        lenient().when(field.getReferenceFormulation()).thenReturn(refFormulation);
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

    private static Join joinConditionWithMaps(ChildMap childMap, ParentMap parentMap) {
        var join = mock(Join.class);
        lenient().when(join.getChildMap()).thenReturn(childMap);
        lenient().when(join.getParentMap()).thenReturn(parentMap);
        return join;
    }

    private static <T extends io.carml.model.ExpressionMap> T referenceMap(Class<T> type, String reference) {
        var map = mock(type);
        lenient().when(map.getReference()).thenReturn(reference);
        return map;
    }

    private static <T extends io.carml.model.ExpressionMap> T templateMap(
            Class<T> type, CarmlTemplate.Segment... segments) {
        var template = mock(Template.class);
        lenient().when(template.getSegments()).thenReturn(List.of(segments));

        var map = mock(type);
        lenient().when(map.getTemplate()).thenReturn(template);
        return map;
    }

    private static <T extends io.carml.model.ExpressionMap> T constantMap(Class<T> type, String value) {
        var map = mock(type);
        lenient()
                .when(map.getConstant())
                .thenReturn(SimpleValueFactory.getInstance().createLiteral(value));
        return map;
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

    private static LogicalView createCsvViewWithMixedFields(String fileName, Set<Field> fields) {
        return createViewWithRefFormulationAndIteratorMixed(Rdf.Ql.Csv, fileName, null, fields);
    }

    private static LogicalView createCsvViewWithJoins(
            String fileName,
            Set<ExpressionField> fields,
            Set<LogicalViewJoin> leftJoins,
            Set<LogicalViewJoin> innerJoins) {
        var view = createViewWithRefFormulationAndIterator(Rdf.Ql.Csv, fileName, null, fields);
        lenient().when(view.getLeftJoins()).thenReturn(leftJoins);
        lenient().when(view.getInnerJoins()).thenReturn(innerJoins);
        return view;
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
        lenient().when(logicalSource.getQuery()).thenReturn(null);
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
        lenient().when(logicalSource.getQuery()).thenReturn(query);
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

    // --- View-on-view composition tests ---

    @Nested
    class ViewOnViewCompilation {

        @Test
        void compile_viewOnView_compilesInnerViewAsSubquery() {
            // Inner view: JSON source with field "name" referencing "$.name"
            var innerView = createJsonView("people.json", "$.people[*]", Set.of(expressionField("name", "$.name")));

            // Outer view: references inner view's "name" column and renames it to "newName"
            var outerView = createViewOnView(innerView, Set.of(expressionField("newName", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(outerView, context).sql();

            // The inner view's CTE query must be parenthesized as a derived table
            assertThat(sql, containsString("(with "));
            // The inner view's SQL should appear as a subquery within the outer query
            assertThat(sql, containsString("read_text('people.json')"));
            assertThat(sql, containsString("json_extract(content, '$.people[*]')"));
            // The outer view should reference "name" as a column (via ColumnSourceStrategy)
            assertThat(sql, containsString("\"view_source\".\"name\" \"newName\""));
        }

        @Test
        void compile_viewOnView_usesColumnStrategyForOuterFields() {
            // Inner view: JSON source with field "age" from JSON
            var innerView = createJsonView("data.json", null, Set.of(expressionField("age", "age")));

            // Outer view: references inner view's "age" column
            var outerView = createViewOnView(innerView, Set.of(expressionField("person_age", "age")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(outerView, context).sql();

            // ColumnSourceStrategy produces direct column references, not json_extract_string
            assertThat(sql, containsString("\"view_source\".\"age\" \"person_age\""));
            assertThat(sql, not(containsString("json_extract_string")));
        }

        @Test
        void compile_viewOnViewCycle_throwsIllegalArgument() {
            // Create expression fields before mocks to avoid UnfinishedStubbingException
            var f1 = expressionField("f1", "f2");
            var f2 = expressionField("f2", "f1");
            var fields1 = castFieldSet(Set.of(f1));
            var fields2 = castFieldSet(Set.of(f2));

            // Create two views that reference each other
            var view1 = mock(LogicalView.class);
            var view2 = mock(LogicalView.class);

            lenient().when(view1.getViewOn()).thenReturn(view2);
            lenient().when(view1.getResourceName()).thenReturn("view1");
            lenient().when(view1.getFields()).thenReturn(fields1);
            lenient().when(view1.getStructuralAnnotations()).thenReturn(Set.of());

            lenient().when(view2.getViewOn()).thenReturn(view1);
            lenient().when(view2.getResourceName()).thenReturn("view2");
            lenient().when(view2.getFields()).thenReturn(fields2);
            lenient().when(view2.getStructuralAnnotations()).thenReturn(Set.of());

            var context = EvaluationContext.defaults();

            var ex = assertThrows(IllegalArgumentException.class, () -> DuckDbViewCompiler.compile(view1, context));
            assertThat(ex.getMessage(), containsString("Cycle detected"));
        }

        @Test
        void compile_viewOnView_projectsTypeCompanionFromInnerView() {
            var innerView = createJsonView("people.json", "$.people[*]", Set.of(expressionField("name", "$.name")));
            var outerView = createViewOnView(innerView, Set.of(expressionField("person_name", "name")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(outerView, context).sql();

            // Outer ColumnSourceStrategy with hasTypeCompanions=true projects the inner type column
            assertThat(sql, containsString("\"view_source\".\"name.__type\" \"person_name.__type\""));
        }

        @Test
        void compile_threeLevelViewOnViewOnView_compilesCorrectly() {
            var innermost = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var middle = createViewOnView(innermost, Set.of(expressionField("mid_id", "id")));
            var outer = createViewOnView(middle, Set.of(expressionField("out_id", "mid_id")));

            var sql = DuckDbViewCompiler.compile(outer, EvaluationContext.defaults())
                    .sql();

            // Two levels of subquery nesting
            assertThat(sql, containsString("read_json_auto('data.json')"));
            assertThat(sql, containsString("\"view_source\".\"mid_id\" \"out_id\""));
        }
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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

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

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // "region" is not NotNull — LEFT JOIN must be retained
            assertThat(sql, containsString("left outer join"));
        }
    }

    @Nested
    class SourceEvaluationColumn {

        @Test
        void compile_forImplicitViewWithJsonIterator_includesIterColumnAsOutputField() {
            var view = createJsonView("data.json", "$.items[*]", Set.of(expressionField("id", "$.id")));
            var context = EvaluationContext.forImplicitView(null);

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // __iter projected as an output field for source-level expression evaluation
            assertThat(sql, containsString("\"__iter\" \"__iter\""));
        }

        @Test
        void compile_forImplicitViewWithoutIterator_doesNotIncludeIterColumn() {
            var view = createJsonView("data.json", null, Set.of(expressionField("id", "id")));
            var context = EvaluationContext.forImplicitView(null);

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            assertThat(sql, not(containsString("\"__iter\"")));
        }

        @Test
        void compile_defaultContextWithJsonIterator_includesIterColumnAsOutputField() {
            var view = createJsonView("data.json", "$.items[*]", Set.of(expressionField("id", "$.id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // __iter is always projected for JSON iterator sources so that expressions not compiled
            // to SQL can be evaluated per-row via the source evaluation fallback
            assertThat(sql, containsString("\"__iter\" \"__iter\""));
        }

        @Test
        void compile_defaultContextWithCsvSource_doesNotIncludeIterColumn() {
            var view = createCsvView("data.csv", Set.of(expressionField("col1", "column1")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // CSV sources do not have a source evaluation column
            assertThat(sql, not(containsString("\"__iter\"")));
        }
    }

    @Nested
    class SourceEvaluationFallback {

        @Test
        void compile_functionFieldWithJsonIterator_skipsFieldAndIncludesIter() {
            var functionField = mock(ExpressionField.class);
            lenient().when(functionField.getFieldName()).thenReturn("computed");
            lenient().when(functionField.getReference()).thenReturn(null);
            lenient().when(functionField.getTemplate()).thenReturn(null);
            lenient().when(functionField.getConstant()).thenReturn(null);
            lenient().when(functionField.getFunctionValue()).thenReturn(mock(TriplesMap.class));

            var regularField = expressionField("id", "$.id");

            var fields = new LinkedHashSet<Field>();
            fields.add(regularField);
            fields.add(functionField);

            var view = createJsonViewWithFields("data.json", "$.items[*]", fields);
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // Regular field is compiled to SQL
            assertThat(sql, containsString("json_extract_string(\"view_source\".\"__iter\", '$.id') \"id\""));
            // Function-based field is NOT in the SQL (skipped for source evaluation fallback)
            assertThat(sql, not(containsString("\"computed\"")));
            // __iter is included for fallback evaluation
            assertThat(sql, containsString("\"__iter\" \"__iter\""));
        }

        @Test
        void compile_functionFieldWithCsvSource_throwsUnsupportedOperationException() {
            var functionField = mock(ExpressionField.class);
            when(functionField.getFieldName()).thenReturn("computed");
            when(functionField.getReference()).thenReturn(null);
            when(functionField.getTemplate()).thenReturn(null);
            when(functionField.getConstant()).thenReturn(null);
            when(functionField.getFunctionValue()).thenReturn(mock(TriplesMap.class));

            var view = createCsvView("data.csv", Set.of(functionField));
            var context = EvaluationContext.defaults();

            // CSV sources have no source evaluation column, so the UnsupportedOperationException
            // is propagated instead of being caught and skipped
            assertThrows(UnsupportedOperationException.class, () -> DuckDbViewCompiler.compile(view, context));
        }

        @Test
        void compile_allFieldsUnsupported_producesQueryWithNoValueFields() {
            var functionField = mock(ExpressionField.class);
            lenient().when(functionField.getFieldName()).thenReturn("computed");
            lenient().when(functionField.getReference()).thenReturn(null);
            lenient().when(functionField.getTemplate()).thenReturn(null);
            lenient().when(functionField.getConstant()).thenReturn(null);
            lenient().when(functionField.getFunctionValue()).thenReturn(mock(TriplesMap.class));

            var view = createJsonViewWithFields("data.json", "$.items[*]", Set.of(functionField));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context).sql();

            // No value fields in the SQL (all skipped)
            assertThat(sql, not(containsString("\"computed\"")));
            // __iter is still included for fallback evaluation
            assertThat(sql, containsString("\"__iter\" \"__iter\""));
            // Index column is still present
            assertThat(sql, containsString("\"" + DuckDbViewCompiler.INDEX_COLUMN + "\""));
        }
    }
}
