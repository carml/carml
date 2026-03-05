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
import io.carml.model.DatabaseSource;
import io.carml.model.ExpressionField;
import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.FunctionExecution;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.ReferenceFormulation;
import io.carml.model.TriplesMap;
import io.carml.model.impl.CarmlTemplate;
import io.carml.vocab.Rdf;
import java.util.List;
import java.util.Set;
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
        void compile_jsonSourceWithIterator_includesJsonPath() {
            var view = createJsonView("data.json", "$.items[*]", Set.of(expressionField("id", "id")));
            var context = EvaluationContext.defaults();

            var sql = DuckDbViewCompiler.compile(view, context);

            assertThat(sql, containsString("read_json_auto('data.json', json_path = '$.items[*]')"));
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
        void compile_xpathReferenceFormulation_throwsUnsupportedOperationException() {
            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.XPath);

            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);

            var context = EvaluationContext.defaults();

            assertThrows(UnsupportedOperationException.class, () -> DuckDbViewCompiler.compile(view, context));
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

            assertThrows(
                    IllegalArgumentException.class,
                    () -> DuckDbViewCompiler.compile(view, EvaluationContext.defaults()));
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

            assertThrows(
                    IllegalArgumentException.class,
                    () -> DuckDbViewCompiler.compile(view, EvaluationContext.defaults()));
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

            assertThrows(
                    UnsupportedOperationException.class,
                    () -> DuckDbViewCompiler.compile(view, EvaluationContext.defaults()));
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

            assertThrows(
                    UnsupportedOperationException.class,
                    () -> DuckDbViewCompiler.compile(view, EvaluationContext.defaults()));
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

            assertThrows(
                    IllegalArgumentException.class,
                    () -> DuckDbViewCompiler.compile(view, EvaluationContext.defaults()));
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
    }

    // --- FilePath source tests ---

    @Nested
    class FilePathSource {

        @Test
        void compile_filePathSource_resolvesPath() {
            var fields = Set.<io.carml.model.Field>of(expressionField("id", "id"));

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
        void compile_rmlXPathFormulation_throwsUnsupportedOperationException() {
            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(Rdf.Rml.XPath);

            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(logicalSource);

            assertThrows(
                    UnsupportedOperationException.class,
                    () -> DuckDbViewCompiler.compile(view, EvaluationContext.defaults()));
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
                    + ") select \"name\" \"name\", "
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
                    + "select distinct \"name\" \"name\" "
                    + "from \"view_source\""
                    + ") select *, "
                    + "row_number() over () \"__idx\" "
                    + "from \"deduped\" "
                    + "limit 10";

            assertThat(sql, is(expected));
        }
    }

    // --- Helper methods ---

    private static ExpressionField expressionField(String fieldName, String reference) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(reference);
        return field;
    }

    private static LogicalView createJsonView(String fileName, String iterator, Set<ExpressionField> fields) {
        return createViewWithRefFormulationAndIterator(Rdf.Ql.JsonPath, fileName, iterator, fields);
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
            org.eclipse.rdf4j.model.Resource refIri,
            String fileName,
            String iterator,
            Set<? extends ExpressionField> fields) {
        var fileSource = mock(FileSource.class);
        when(fileSource.getUrl()).thenReturn(fileName);

        var refFormulation = mock(ReferenceFormulation.class);
        when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = mock(LogicalSource.class);
        when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        when(logicalSource.getSource()).thenReturn(fileSource);
        lenient().when(logicalSource.getIterator()).thenReturn(iterator);

        var view = mock(LogicalView.class);
        when(view.getViewOn()).thenReturn(logicalSource);
        when(view.getFields()).thenReturn((Set<io.carml.model.Field>) (Set<?>) fields);
        lenient().when(view.getResourceName()).thenReturn("testView");

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

        return view;
    }

    @SuppressWarnings("unchecked")
    private static Set<io.carml.model.Field> castFieldSet(Set<ExpressionField> fields) {
        return (Set<io.carml.model.Field>) (Set<?>) fields;
    }
}
