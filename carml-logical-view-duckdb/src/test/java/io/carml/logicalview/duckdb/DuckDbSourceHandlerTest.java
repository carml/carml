package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.model.DatabaseSource;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.FileSource;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.vocab.Rdf;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DuckDbSourceHandlerTest {

    private static final String CTE_ALIAS = "view_source";

    @Nested
    class ForFormulation {

        @Test
        void qlJsonPath_returnsJsonPathHandler() {
            var result = DuckDbSourceHandler.forFormulation(Rdf.Ql.JsonPath);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get(), instanceOf(JsonPathSourceHandler.class));
        }

        @Test
        void rmlJsonPath_returnsJsonPathHandler() {
            var result = DuckDbSourceHandler.forFormulation(Rdf.Rml.JsonPath);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get(), instanceOf(JsonPathSourceHandler.class));
        }

        @Test
        void qlCsv_returnsCsvHandler() {
            var result = DuckDbSourceHandler.forFormulation(Rdf.Ql.Csv);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get(), instanceOf(CsvSourceHandler.class));
        }

        @Test
        void rmlCsv_returnsCsvHandler() {
            var result = DuckDbSourceHandler.forFormulation(Rdf.Rml.Csv);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get(), instanceOf(CsvSourceHandler.class));
        }

        @Test
        void qlRdb_returnsSqlHandler() {
            var result = DuckDbSourceHandler.forFormulation(Rdf.Ql.Rdb);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get(), instanceOf(SqlSourceHandler.class));
        }

        @Test
        void rmlSql2008Table_returnsSqlHandler() {
            var result = DuckDbSourceHandler.forFormulation(Rdf.Rml.SQL2008Table);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get(), instanceOf(SqlSourceHandler.class));
        }

        @Test
        void rmlSql2008Query_returnsSqlHandler() {
            var result = DuckDbSourceHandler.forFormulation(Rdf.Rml.SQL2008Query);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get(), instanceOf(SqlSourceHandler.class));
        }

        @Test
        void rmlRdb_returnsSqlHandler() {
            var result = DuckDbSourceHandler.forFormulation(Rdf.Rml.Rdb);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get(), instanceOf(SqlSourceHandler.class));
        }

        @Test
        void qlXPath_returnsEmpty() {
            var result = DuckDbSourceHandler.forFormulation(Rdf.Ql.XPath);

            assertThat(result, is(Optional.empty()));
        }

        @Test
        void unknownIri_returnsEmpty() {
            var unknownIri = SimpleValueFactory.getInstance().createIRI("http://example.org/unknown");
            var result = DuckDbSourceHandler.forFormulation(unknownIri);

            assertThat(result, is(Optional.empty()));
        }
    }

    @Nested
    class JsonPathHandlerIsCompatible {

        private final JsonPathSourceHandler handler = new JsonPathSourceHandler();

        @Test
        void fileSource_returnsTrue() {
            var logicalSource = mockLogicalSourceWithFileSource("data.json");

            assertThat(handler.isCompatible(logicalSource), is(true));
        }

        @Test
        void streamSource_returnsFalse() {
            var source = mock(Source.class);
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getSource()).thenReturn(source);

            assertThat(handler.isCompatible(logicalSource), is(false));
        }

        @Test
        void recursiveDescent_returnsFalse() {
            var logicalSource = mockLogicalSourceWithFileSource("data.json");
            when(logicalSource.getIterator()).thenReturn("$.store..price");

            assertThat(handler.isCompatible(logicalSource), is(false));
        }

        @Test
        void nullIterator_returnsTrue() {
            var logicalSource = mockLogicalSourceWithFileSource("data.json");
            when(logicalSource.getIterator()).thenReturn(null);

            assertThat(handler.isCompatible(logicalSource), is(true));
        }

        @Test
        void blankIterator_returnsTrue() {
            var logicalSource = mockLogicalSourceWithFileSource("data.json");
            when(logicalSource.getIterator()).thenReturn("   ");

            assertThat(handler.isCompatible(logicalSource), is(true));
        }
    }

    @Nested
    class CsvHandlerIsCompatible {

        private final CsvSourceHandler handler = new CsvSourceHandler();

        @Test
        void fileSource_returnsTrue() {
            var logicalSource = mockLogicalSourceWithFileSource("data.csv");

            assertThat(handler.isCompatible(logicalSource), is(true));
        }

        @Test
        void streamSource_returnsFalse() {
            var source = mock(Source.class);
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getSource()).thenReturn(source);

            assertThat(handler.isCompatible(logicalSource), is(false));
        }
    }

    @Nested
    class SqlHandlerIsCompatible {

        private final SqlSourceHandler handler = new SqlSourceHandler();

        @Test
        void alwaysReturnsTrue() {
            var logicalSource = mock(LogicalSource.class);

            assertThat(handler.isCompatible(logicalSource), is(true));
        }
    }

    @Nested
    class JsonPathHandlerCompileSource {

        private final JsonPathSourceHandler handler = new JsonPathSourceHandler();

        @Test
        void iteratorSource_producesJsonExtractUnnest() {
            var logicalSource = mockLogicalSourceWithFileSource("data.json");
            when(logicalSource.getIterator()).thenReturn("$.people[*]");
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), containsString("json_extract"));
            assertThat(result.sourceSql(), containsString("unnest"));
            assertThat(result.sourceSql(), containsString("read_text"));
            assertThat(result.strategy(), instanceOf(JsonIteratorSourceStrategy.class));
        }

        @Test
        void nonIteratorSource_producesReadJsonAuto() {
            var logicalSource = mockLogicalSourceWithFileSource("data.json");
            when(logicalSource.getIterator()).thenReturn(null);
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), containsString("read_json_auto"));
            assertThat(result.strategy(), instanceOf(ColumnSourceStrategy.class));
        }

        @Test
        void parquetFile_producesReadParquet() {
            var logicalSource = mockLogicalSourceWithFileSource("data.parquet");
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), containsString("read_parquet"));
            assertThat(result.strategy(), instanceOf(ColumnSourceStrategy.class));
        }

        @Test
        void iteratorWithFilter_producesWhereClause() {
            var logicalSource = mockLogicalSourceWithFileSource("data.json");
            when(logicalSource.getIterator()).thenReturn("$.people[?(@.active==true)]");
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), containsString("where"));
            assertThat(result.strategy(), instanceOf(JsonIteratorSourceStrategy.class));
        }
    }

    @Nested
    class CsvHandlerCompileSource {

        private final CsvSourceHandler handler = new CsvSourceHandler();

        @Test
        void csvFile_producesReadCsvAuto() {
            var logicalSource = mockLogicalSourceWithFileSource("data.csv");
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), containsString("read_csv_auto"));
            assertThat(result.strategy(), instanceOf(ColumnSourceStrategy.class));
        }

        @Test
        void parquetFile_producesReadParquet() {
            var logicalSource = mockLogicalSourceWithFileSource("data.parquet");
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), containsString("read_parquet"));
            assertThat(result.strategy(), instanceOf(ColumnSourceStrategy.class));
        }
    }

    @Nested
    class SqlHandlerCompileSource {

        private final SqlSourceHandler handler = new SqlSourceHandler();

        @Test
        void querySource_wrapsAsSubquery() {
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getQuery()).thenReturn("SELECT * FROM people");
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), is("(SELECT * FROM people)"));
            assertThat(result.strategy(), instanceOf(ColumnSourceStrategy.class));
        }

        @Test
        void tableNameSource_producesQuotedName() {
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getQuery()).thenReturn(null);
            when(logicalSource.getTableName()).thenReturn("employees");
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), containsString("employees"));
            assertThat(result.strategy(), instanceOf(ColumnSourceStrategy.class));
        }

        @Test
        void databaseSource_wrapsAsSubquery() {
            var dbSource = mock(DatabaseSource.class);
            when(dbSource.getQuery()).thenReturn("SELECT id, name FROM departments");
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getQuery()).thenReturn(null);
            when(logicalSource.getTableName()).thenReturn(null);
            when(logicalSource.getSource()).thenReturn(dbSource);
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), is("(SELECT id, name FROM departments)"));
            assertThat(result.strategy(), instanceOf(ColumnSourceStrategy.class));
        }

        @Test
        void blankQuery_fallsThroughToTableName() {
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getQuery()).thenReturn("   ");
            when(logicalSource.getTableName()).thenReturn("employees");
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), containsString("employees"));
        }

        @Test
        void blankTableName_fallsThroughToDatabaseSource() {
            var dbSource = mock(DatabaseSource.class);
            when(dbSource.getQuery()).thenReturn("SELECT 1");
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getQuery()).thenReturn(null);
            when(logicalSource.getTableName()).thenReturn("  ");
            when(logicalSource.getSource()).thenReturn(dbSource);
            var fields = Set.<Field>of(expressionField("name", "name"));

            var result = handler.compileSource(logicalSource, fields, CTE_ALIAS);

            assertThat(result.sourceSql(), is("(SELECT 1)"));
        }

        @Test
        void noQueryOrTableName_throws() {
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getQuery()).thenReturn(null);
            when(logicalSource.getTableName()).thenReturn(null);
            when(logicalSource.getSource()).thenReturn(null);
            var fields = Set.<Field>of(expressionField("name", "name"));

            assertThrows(IllegalArgumentException.class, () -> handler.compileSource(logicalSource, fields, CTE_ALIAS));
        }

        @Test
        void databaseSourceWithNullQuery_throws() {
            var dbSource = mock(DatabaseSource.class);
            when(dbSource.getQuery()).thenReturn(null);
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getQuery()).thenReturn(null);
            when(logicalSource.getTableName()).thenReturn(null);
            when(logicalSource.getSource()).thenReturn(dbSource);
            var fields = Set.<Field>of(expressionField("name", "name"));

            assertThrows(IllegalArgumentException.class, () -> handler.compileSource(logicalSource, fields, CTE_ALIAS));
        }
    }

    // --- Test helpers ---

    private static LogicalSource mockLogicalSourceWithFileSource(String url) {
        var fileSource = mock(FileSource.class);
        when(fileSource.getUrl()).thenReturn(url);
        var logicalSource = mock(LogicalSource.class);
        when(logicalSource.getSource()).thenReturn(fileSource);
        return logicalSource;
    }

    private static ExpressionField expressionField(String fieldName, String reference) {
        var field = mock(ExpressionField.class);
        when(field.getFieldName()).thenReturn(fieldName);
        when(field.getReference()).thenReturn(reference);
        return field;
    }
}
