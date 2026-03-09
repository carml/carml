package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import io.carml.model.FileSource;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NameableStream;
import io.carml.model.ReferenceFormulation;
import io.carml.vocab.Rdf;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DuckDbLogicalViewEvaluatorFactoryTest {

    private static Connection connection;

    @BeforeAll
    static void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // --- Compatible source matching ---

    @Nested
    class CompatibleSourceMatching {

        @Test
        void match_jsonSource_returnsStrongMatch() {
            var view = createViewWithRefFormulation(Rdf.Ql.JsonPath);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
            assertThat(result.get().getLogicalViewEvaluator(), instanceOf(DuckDbLogicalViewEvaluator.class));
        }

        @Test
        void match_rmlJsonPathSource_returnsStrongMatch() {
            var view = createViewWithRefFormulation(Rdf.Rml.JsonPath);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_csvSource_returnsStrongMatch() {
            var view = createViewWithRefFormulation(Rdf.Ql.Csv);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_rmlCsvSource_returnsStrongMatch() {
            var view = createViewWithRefFormulation(Rdf.Rml.Csv);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_rdbSource_returnsStrongMatch() {
            var view = createViewWithRefFormulation(Rdf.Ql.Rdb);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_sql2008TableSource_returnsStrongMatch() {
            var view = createViewWithRefFormulation(Rdf.Rml.SQL2008Table);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_sql2008QuerySource_returnsStrongMatch() {
            var view = createViewWithRefFormulation(Rdf.Rml.SQL2008Query);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }
    }

    // --- Incompatible source matching ---

    @Nested
    class IncompatibleSourceMatching {

        @Test
        void match_xPathSource_returnsEmpty() {
            var view = createViewWithRefFormulation(Rdf.Ql.XPath);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_rmlXPathSource_returnsEmpty() {
            var view = createViewWithRefFormulation(Rdf.Rml.XPath);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_unknownRefFormulation_returnsEmpty() {
            var unknownIri = org.eclipse.rdf4j.model.impl.SimpleValueFactory.getInstance()
                    .createIRI("http://example.org/SparqlResults");
            var view = createViewWithRefFormulation(unknownIri);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_nullViewOn_returnsEmpty() {
            var view = mock(LogicalView.class);
            lenient().when(view.getViewOn()).thenReturn(null);
            lenient().when(view.getLeftJoins()).thenReturn(Set.of());
            lenient().when(view.getInnerJoins()).thenReturn(Set.of());
            lenient().when(view.getResourceName()).thenReturn("nullViewOnView");

            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_csvWithStreamSource_returnsEmpty() {
            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.Csv);

            var streamSource = mock(NameableStream.class);

            var logicalSource = mock(LogicalSource.class);
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(streamSource);

            var view = createViewWithLogicalSource(logicalSource);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_jsonWithStreamSource_returnsEmpty() {
            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

            var streamSource = mock(NameableStream.class);

            var logicalSource = mock(LogicalSource.class);
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(streamSource);

            var view = createViewWithLogicalSource(logicalSource);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_nullRefFormulation_returnsEmpty() {
            var logicalSource = mock(LogicalSource.class);
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(null);

            var view = createViewWithLogicalSource(logicalSource);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }
    }

    // --- Recursive view tree walking ---

    @Nested
    class RecursiveViewTreeWalking {

        @Test
        void match_viewOnNestedLogicalView_allCompatible_returnsStrongMatch() {
            var innerView = createViewWithRefFormulation(Rdf.Ql.JsonPath);

            // Outer view whose viewOn is the inner LogicalView
            var outerView = mock(LogicalView.class);
            lenient().when(outerView.getViewOn()).thenReturn(innerView);
            lenient().when(outerView.getFields()).thenReturn(Set.of());
            lenient().when(outerView.getLeftJoins()).thenReturn(Set.of());
            lenient().when(outerView.getInnerJoins()).thenReturn(Set.of());
            lenient().when(outerView.getStructuralAnnotations()).thenReturn(Set.of());
            lenient().when(outerView.getResourceName()).thenReturn("outerView");

            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(outerView);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_leftJoinParentView_compatible_returnsStrongMatch() {
            var parentView = createViewWithRefFormulation(Rdf.Ql.Csv);
            var join = mockJoin(parentView);

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(join), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_innerJoinParentView_compatible_returnsStrongMatch() {
            var parentView = createViewWithRefFormulation(Rdf.Ql.Rdb);
            var join = mockJoin(parentView);

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(), Set.of(join));
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_leftJoinParentView_incompatible_returnsEmpty() {
            var parentView = createViewWithRefFormulation(Rdf.Ql.XPath);
            var join = mockJoin(parentView);

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(join), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_innerJoinParentView_incompatible_returnsEmpty() {
            var parentView = createViewWithRefFormulation(Rdf.Ql.XPath);
            var join = mockJoin(parentView);

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.Csv, Set.of(), Set.of(join));
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_multipleJoins_allCompatible_returnsStrongMatch() {
            var parentView1 = createViewWithRefFormulation(Rdf.Ql.Csv);
            var parentView2 = createViewWithRefFormulation(Rdf.Ql.Rdb);
            var join1 = mockJoin(parentView1);
            var join2 = mockJoin(parentView2);

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(join1), Set.of(join2));
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_cyclicViewGraph_doesNotStackOverflow() {
            var cyclicView = mock(LogicalView.class);
            var selfJoin = mockJoin(cyclicView);

            var fileSource = mock(FileSource.class);
            lenient().when(fileSource.getUrl()).thenReturn("/test/path/file.json");

            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);
            var logicalSource = mock(LogicalSource.class);
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(fileSource);

            lenient().when(cyclicView.getViewOn()).thenReturn(logicalSource);
            lenient().when(cyclicView.getLeftJoins()).thenReturn(Set.of(selfJoin));
            lenient().when(cyclicView.getInnerJoins()).thenReturn(Set.of());
            lenient().when(cyclicView.getResourceName()).thenReturn("cyclicView");

            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(cyclicView);

            assertThat(result.isPresent(), is(true));
        }

        @Test
        void match_deeplyNestedJoinParent_incompatible_returnsEmpty() {
            // Parent of the parent has an XPath source (incompatible)
            var deepParent = createViewWithRefFormulation(Rdf.Ql.XPath);
            var deepJoin = mockJoin(deepParent);

            // Intermediate parent: JSON source, but joins to deep parent
            var intermediateParent = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(deepJoin), Set.of());
            var intermediateJoin = mockJoin(intermediateParent);

            // Top-level view: CSV source, joins to intermediate parent
            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.Csv, Set.of(intermediateJoin), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }
    }

    // --- Mixed compatible/incompatible sources ---

    @Nested
    class MixedSourceMatching {

        @Test
        void match_compatibleRootIncompatibleJoinParent_returnsEmpty() {
            var parentView = createViewWithRefFormulation(Rdf.Ql.XPath);
            var join = mockJoin(parentView);

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(join), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_incompatibleRootCompatibleJoinParent_returnsEmpty() {
            var parentView = createViewWithRefFormulation(Rdf.Ql.JsonPath);
            var join = mockJoin(parentView);

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.XPath, Set.of(join), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_crossFormatCompatible_returnsStrongMatch() {
            // JSON root view joining with CSV parent -- both compatible
            var parentView = createViewWithRefFormulation(Rdf.Ql.Csv);
            var join = mockJoin(parentView);

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(join), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }
    }

    // --- Zero-arg constructor ---

    @Nested
    class ZeroArgConstructor {

        @Test
        void match_zeroArgConstructor_createsWorkingFactory() {
            try (var factory = new DuckDbLogicalViewEvaluatorFactory()) {
                var view = createViewWithRefFormulation(Rdf.Ql.JsonPath);

                var result = factory.match(view);

                assertThat(result.isPresent(), is(true));
                assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
            }
        }
    }

    // --- Helper methods ---

    private static int strongScore() {
        return MatchScore.builder().strongMatch().build().getScore();
    }

    private static LogicalView createViewWithRefFormulation(Resource refIri) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn("/test/path/file.json");

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getSource()).thenReturn(fileSource);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        lenient().when(view.getFields()).thenReturn(Set.of());
        lenient().when(view.getLeftJoins()).thenReturn(Set.of());
        lenient().when(view.getInnerJoins()).thenReturn(Set.of());
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());
        lenient().when(view.getResourceName()).thenReturn("testView");

        return view;
    }

    private static LogicalView createViewWithLogicalSource(LogicalSource logicalSource) {
        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        lenient().when(view.getFields()).thenReturn(Set.of());
        lenient().when(view.getLeftJoins()).thenReturn(Set.of());
        lenient().when(view.getInnerJoins()).thenReturn(Set.of());
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());
        lenient().when(view.getResourceName()).thenReturn("testView");

        return view;
    }

    private static LogicalView createViewWithRefFormulationAndJoins(
            Resource refIri, Set<LogicalViewJoin> leftJoins, Set<LogicalViewJoin> innerJoins) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn("/test/path/file.json");

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getSource()).thenReturn(fileSource);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        lenient().when(view.getFields()).thenReturn(Set.of());
        lenient().when(view.getLeftJoins()).thenReturn(leftJoins);
        lenient().when(view.getInnerJoins()).thenReturn(innerJoins);
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());
        lenient().when(view.getResourceName()).thenReturn("testView");

        return view;
    }

    private static LogicalViewJoin mockJoin(LogicalView parentView) {
        var join = mock(LogicalViewJoin.class);
        lenient().when(join.getParentLogicalView()).thenReturn(parentView);
        lenient().when(join.getJoinConditions()).thenReturn(Set.of());
        lenient().when(join.getFields()).thenReturn(Set.of());
        return join;
    }
}
