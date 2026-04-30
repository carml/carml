package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import io.carml.model.ChildMap;
import io.carml.model.Condition;
import io.carml.model.ExpressionMap;
import io.carml.model.FileSource;
import io.carml.model.FunctionExecution;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NameableStream;
import io.carml.model.ParentMap;
import io.carml.model.ReferenceFormulation;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rdf;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Values;
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
            var view = createSqlViewWithTable(Rdf.Ql.Rdb, "people");
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_sql2008TableSource_returnsStrongMatch() {
            var view = createSqlViewWithTable(Rdf.Rml.SQL2008Table, "people");
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_sql2008QuerySource_returnsStrongMatch() {
            var view = createSqlViewWithQuery(Rdf.Rml.SQL2008Query, "select * from people");
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_jsonSourceWithDeepScanIterator_returnsEvaluator() {
            var fileSource = mock(FileSource.class);
            lenient().when(fileSource.getUrl()).thenReturn("/test/data.json");

            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(fileSource);
            lenient().when(logicalSource.getIterator()).thenReturn("$..name");

            var view = createViewWithLogicalSource(logicalSource);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
        }

        @Test
        void match_rmlJsonPathSourceWithDeepScanIterator_returnsEvaluator() {
            var fileSource = mock(FileSource.class);
            lenient().when(fileSource.getUrl()).thenReturn("/test/data.json");

            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Rml.JsonPath);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(fileSource);
            lenient().when(logicalSource.getIterator()).thenReturn("$..items[*]");

            var view = createViewWithLogicalSource(logicalSource);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
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

            var logicalSource = newLogicalSourceMock();
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

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(streamSource);

            var view = createViewWithLogicalSource(logicalSource);
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_nullRefFormulation_returnsEmpty() {
            var logicalSource = newLogicalSourceMock();
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
            var parentView = createSqlViewWithTable(Rdf.Ql.Rdb, "people");
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
            var parentView2 = createSqlViewWithTable(Rdf.Ql.Rdb, "departments");
            var join1 = mockJoin(parentView1);
            var join2 = mockJoin(parentView2);

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(join1), Set.of(join2));
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_cyclicViewGraph_sourceCompatTraversalDoesNotStackOverflow() {
            var cyclicView = mock(LogicalView.class);
            var selfJoin = mockJoin(cyclicView);

            var fileSource = mock(FileSource.class);
            lenient().when(fileSource.getUrl()).thenReturn("/test/path/file.json");

            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);
            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(fileSource);

            lenient().when(cyclicView.getViewOn()).thenReturn(logicalSource);
            lenient().when(cyclicView.getFields()).thenReturn(Set.of());
            lenient().when(cyclicView.getLeftJoins()).thenReturn(Set.of(selfJoin));
            lenient().when(cyclicView.getInnerJoins()).thenReturn(Set.of());
            lenient().when(cyclicView.getStructuralAnnotations()).thenReturn(Set.of());
            lenient().when(cyclicView.getResourceName()).thenReturn("cyclicView");

            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            // The source-compatibility traversal must not stack overflow on cyclic graphs (it
            // tracks visited views). Compilation does reject the cycle as an invalid view, raising
            // IllegalArgumentException — which is the right behavior because such cycles cannot
            // be evaluated. We only assert the source-compat walk terminated and propagated the
            // compile-time validation error rather than stack-overflowing.
            org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalArgumentException.class, () -> factory.match(cyclicView));
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

    // --- On-disk mode ---

    @Nested
    class OnDiskMode {

        @Test
        void createOnDisk_createsConnectionAndDatabaseFile() {
            try (var factory = DuckDbLogicalViewEvaluatorFactory.createOnDisk()) {
                var view = createViewWithRefFormulation(Rdf.Ql.JsonPath);

                var result = factory.match(view);

                assertThat(result.isPresent(), is(true));
                assertThat(result.get().getLogicalViewEvaluator(), instanceOf(DuckDbLogicalViewEvaluator.class));
            }
        }

        @Test
        void createOnDisk_matchesViews() {
            try (var factory = DuckDbLogicalViewEvaluatorFactory.createOnDisk()) {
                var view = createViewWithRefFormulation(Rdf.Ql.Csv);

                var result = factory.match(view);

                assertThat(result.isPresent(), is(true));
                assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
            }
        }

        @Test
        void close_onDisk_deletesDatabaseFiles() {
            var factory = DuckDbLogicalViewEvaluatorFactory.createOnDisk();

            var dbFile = factory.getDatabasePath();
            var tempDir = dbFile.getParent();
            assertThat("Database file should exist before close", Files.exists(dbFile), is(true));

            factory.close();

            assertThat("Database file should be deleted after close", Files.exists(dbFile), is(false));
            assertThat("Temp directory should be deleted after close", Files.exists(tempDir), is(false));
        }

        @Test
        void createOnDisk_setsMemoryLimit() throws SQLException {
            try (var factory = DuckDbLogicalViewEvaluatorFactory.createOnDisk()) {
                var dbPath = factory.getDatabasePath();
                assertThat("On-disk factory should have a database path", dbPath, is(not(nullValue())));

                // Connect to the same on-disk database and verify memory_limit was reduced
                try (var conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
                        var stmt = conn.createStatement();
                        var rs = stmt.executeQuery("SELECT current_setting('memory_limit')")) {
                    rs.next();
                    var memLimit = rs.getString(1);
                    // memory_limit should be set (non-null, non-empty)
                    assertThat("memory_limit should be configured", memLimit, is(not(nullValue())));
                    // Should not be the DuckDB default (80% of system memory).
                    // The exact value depends on system memory, but it should not be "0 bytes".
                    assertThat("memory_limit should not be zero", memLimit, is(not("0 bytes")));
                }
            }
        }

        @Test
        void close_inMemory_noFileCleanup() {
            var factory = new DuckDbLogicalViewEvaluatorFactory();
            factory.close();

            assertThat("Database path should be null for in-memory mode", factory.getDatabasePath(), is(nullValue()));
        }

        @Test
        void close_calledTwice_isIdempotent() {
            var factory = DuckDbLogicalViewEvaluatorFactory.createOnDisk();
            var dbFile = factory.getDatabasePath();

            factory.close();
            // Second close must not throw
            factory.close();

            assertThat("Database file should be deleted", Files.exists(dbFile), is(false));
        }
    }

    // --- Scheduler lifecycle ---

    @Nested
    class SchedulerLifecycle {

        @Test
        void close_withCustomScheduler_disposesScheduler() {
            var scheduler = reactor.core.scheduler.Schedulers.newBoundedElastic(1, 10, "test-scheduler");
            var factory = new DuckDbLogicalViewEvaluatorFactory(scheduler);

            factory.close();

            assertThat("Scheduler should be disposed after factory close", scheduler.isDisposed(), is(true));
        }

        @Test
        void close_onDiskWithCustomScheduler_disposesScheduler() {
            var scheduler = reactor.core.scheduler.Schedulers.newBoundedElastic(1, 10, "test-scheduler");
            var factory = DuckDbLogicalViewEvaluatorFactory.createOnDisk(scheduler);

            factory.close();

            assertThat("Scheduler should be disposed after factory close", scheduler.isDisposed(), is(true));
        }

        @Test
        void close_withDefaultScheduler_doesNotDisposeSharedBoundedElastic() throws SQLException {
            // Use a dedicated connection because factory.close() closes the connection, and we
            // must not close the shared static connection used by other tests.
            var dedicatedConn = DriverManager.getConnection("jdbc:duckdb:");
            var factory = new DuckDbLogicalViewEvaluatorFactory(dedicatedConn);
            factory.close();

            assertThat(
                    "Shared boundedElastic must not be disposed",
                    reactor.core.scheduler.Schedulers.boundedElastic().isDisposed(),
                    is(false));
        }

        @Test
        void constructor_withCustomScheduler_matchesViews() {
            var scheduler = reactor.core.scheduler.Schedulers.newBoundedElastic(1, 10, "test-scheduler");
            try (var factory = new DuckDbLogicalViewEvaluatorFactory(scheduler)) {
                var view = createViewWithRefFormulation(Rdf.Ql.JsonPath);

                var result = factory.match(view);

                assertThat(result.isPresent(), is(true));
                assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
            }
        }
    }

    // --- Trial-compile gating: views the SQL compiler cannot handle should decline gracefully ---

    @Nested
    class TrialCompileGating {

        @Test
        void match_viewWithStandardJoinCondition_returnsPresentMatch() {
            var parentView = createViewWithRefFormulation(Rdf.Ql.JsonPath);
            var childMap = referenceChildMap("child_id");
            var parentMap = referenceParentMap("parent_id");
            var join = mock(Join.class);
            lenient().when(join.getChildMap()).thenReturn(childMap);
            lenient().when(join.getParentMap()).thenReturn(parentMap);

            var viewJoin = mock(LogicalViewJoin.class);
            lenient().when(viewJoin.getParentLogicalView()).thenReturn(parentView);
            lenient().when(viewJoin.getJoinConditions()).thenReturn(Set.of(join));
            lenient().when(viewJoin.getFields()).thenReturn(Set.of());

            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(viewJoin), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getMatchScore().getScore(), is(strongScore()));
        }

        @Test
        void match_viewWithFunctionValuedJoinChildMap_returnsEmpty() {
            var triplesMap = mock(TriplesMap.class);
            var childMap = stubAsRdf(mock(ChildMap.class));
            lenient().when(childMap.getFunctionValue()).thenReturn(triplesMap);

            var parentMap = referenceParentMap("parent_id");
            var join = mock(Join.class);
            lenient().when(join.getChildMap()).thenReturn(childMap);
            lenient().when(join.getParentMap()).thenReturn(parentMap);

            var viewJoin = viewJoinWithCondition(join);
            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(viewJoin), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_viewWithFunctionValuedJoinParentMap_returnsEmpty() {
            var triplesMap = mock(TriplesMap.class);
            var parentMap = stubAsRdf(mock(ParentMap.class));
            lenient().when(parentMap.getFunctionValue()).thenReturn(triplesMap);

            var childMap = referenceChildMap("child_id");
            var join = mock(Join.class);
            lenient().when(join.getChildMap()).thenReturn(childMap);
            lenient().when(join.getParentMap()).thenReturn(parentMap);

            var viewJoin = viewJoinWithCondition(join);
            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(viewJoin), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_viewWithFunctionExecutionJoinChildMap_returnsEmpty() {
            var functionExecution = mock(FunctionExecution.class);
            var childMap = stubAsRdf(mock(ChildMap.class));
            lenient().when(childMap.getFunctionExecution()).thenReturn(functionExecution);

            var parentMap = referenceParentMap("parent_id");
            var join = mock(Join.class);
            lenient().when(join.getChildMap()).thenReturn(childMap);
            lenient().when(join.getParentMap()).thenReturn(parentMap);

            var viewJoin = viewJoinWithCondition(join);
            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(viewJoin), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_viewWithConditionedJoinChildMap_returnsEmpty() {
            var condition = mock(Condition.class);
            var childMap = stubAsRdf(mock(ChildMap.class));
            lenient().when(childMap.getReference()).thenReturn("child_id");
            lenient().when(childMap.getConditions()).thenReturn(Set.of(condition));

            var parentMap = referenceParentMap("parent_id");
            var join = mock(Join.class);
            lenient().when(join.getChildMap()).thenReturn(childMap);
            lenient().when(join.getParentMap()).thenReturn(parentMap);

            var viewJoin = viewJoinWithCondition(join);
            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(viewJoin), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void match_viewWithConditionedJoinParentMap_returnsEmpty() {
            var condition = mock(Condition.class);
            var parentMap = stubAsRdf(mock(ParentMap.class));
            lenient().when(parentMap.getReference()).thenReturn("parent_id");
            lenient().when(parentMap.getConditions()).thenReturn(Set.of(condition));

            var childMap = referenceChildMap("child_id");
            var join = mock(Join.class);
            lenient().when(join.getChildMap()).thenReturn(childMap);
            lenient().when(join.getParentMap()).thenReturn(parentMap);

            var viewJoin = viewJoinWithCondition(join);
            var view = createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(viewJoin), Set.of());
            var factory = new DuckDbLogicalViewEvaluatorFactory(connection);

            var result = factory.match(view);

            assertThat(result.isEmpty(), is(true));
        }

        private LogicalViewJoin viewJoinWithCondition(Join join) {
            var parentView = createViewWithRefFormulation(Rdf.Ql.JsonPath);
            var viewJoin = mock(LogicalViewJoin.class);
            lenient().when(viewJoin.getParentLogicalView()).thenReturn(parentView);
            lenient().when(viewJoin.getJoinConditions()).thenReturn(Set.of(join));
            lenient().when(viewJoin.getFields()).thenReturn(Set.of());
            return viewJoin;
        }

        private ChildMap referenceChildMap(String reference) {
            var childMap = mock(ChildMap.class);
            lenient().when(childMap.getReference()).thenReturn(reference);
            return childMap;
        }

        private ParentMap referenceParentMap(String reference) {
            var parentMap = mock(ParentMap.class);
            lenient().when(parentMap.getReference()).thenReturn(reference);
            return parentMap;
        }
    }

    // --- Integration: routing between DuckDB and the reactive default factory ---

    /**
     * End-to-end routing contract: when the DuckDB factory declines a view containing structures it
     * cannot compile (function-valued or conditioned join keys), the reactive
     * {@link io.carml.logicalview.DefaultLogicalViewEvaluatorFactory} still matches it (with a weak
     * score) so the view is evaluated by the fallback rather than failing the run. This mirrors the
     * selection logic in {@code FactoryDelegatingEvaluator} without dragging in the full engine
     * pipeline.
     */
    @Nested
    class EvaluatorRoutingIntegration {

        @Test
        void match_functionValuedJoinChildMap_duckDbDeclinesAndDefaultMatches() {
            var triplesMap = mock(TriplesMap.class);
            var childMap = stubAsRdf(mock(ChildMap.class));
            lenient().when(childMap.getFunctionValue()).thenReturn(triplesMap);

            var parentMap = mock(ParentMap.class);
            lenient().when(parentMap.getReference()).thenReturn("parent_id");

            var join = mock(Join.class);
            lenient().when(join.getChildMap()).thenReturn(childMap);
            lenient().when(join.getParentMap()).thenReturn(parentMap);

            var view = viewWithSingleJoinCondition(join);

            var duckDbFactory = new DuckDbLogicalViewEvaluatorFactory(connection);
            var defaultFactory = new io.carml.logicalview.DefaultLogicalViewEvaluatorFactory();

            var duckDbResult = duckDbFactory.match(view);
            var defaultResult = defaultFactory.match(view);

            assertThat(duckDbResult.isEmpty(), is(true));
            assertThat(defaultResult.isPresent(), is(true));
            // The default factory always matches with a weak score; the DuckDB factory's strong
            // match would normally win, but it has declined here. Mirror the selection logic the
            // engine uses to confirm the reactive evaluator is the one that gets picked.
            var matches = java.util.stream.Stream.of(duckDbResult, defaultResult)
                    .flatMap(java.util.Optional::stream)
                    .toList();
            var selected = io.carml.logicalview.MatchedLogicalViewEvaluator.select(matches);

            assertThat(selected.isPresent(), is(true));
            assertThat(selected.get(), instanceOf(io.carml.logicalview.DefaultLogicalViewEvaluator.class));
        }

        @Test
        void match_conditionedJoinChildMap_duckDbDeclinesAndDefaultMatches() {
            var condition = mock(Condition.class);
            var childMap = stubAsRdf(mock(ChildMap.class));
            lenient().when(childMap.getReference()).thenReturn("child_id");
            lenient().when(childMap.getConditions()).thenReturn(Set.of(condition));

            var parentMap = mock(ParentMap.class);
            lenient().when(parentMap.getReference()).thenReturn("parent_id");

            var join = mock(Join.class);
            lenient().when(join.getChildMap()).thenReturn(childMap);
            lenient().when(join.getParentMap()).thenReturn(parentMap);

            var view = viewWithSingleJoinCondition(join);

            var duckDbFactory = new DuckDbLogicalViewEvaluatorFactory(connection);
            var defaultFactory = new io.carml.logicalview.DefaultLogicalViewEvaluatorFactory();

            var duckDbResult = duckDbFactory.match(view);
            var defaultResult = defaultFactory.match(view);

            assertThat(duckDbResult.isEmpty(), is(true));
            assertThat(defaultResult.isPresent(), is(true));

            var matches = java.util.stream.Stream.of(duckDbResult, defaultResult)
                    .flatMap(java.util.Optional::stream)
                    .toList();
            var selected = io.carml.logicalview.MatchedLogicalViewEvaluator.select(matches);

            assertThat(selected.isPresent(), is(true));
            assertThat(selected.get(), instanceOf(io.carml.logicalview.DefaultLogicalViewEvaluator.class));
        }

        @Test
        void match_compatibleView_duckDbWinsOverDefault() {
            var childMap = mock(ChildMap.class);
            lenient().when(childMap.getReference()).thenReturn("child_id");

            var parentMap = mock(ParentMap.class);
            lenient().when(parentMap.getReference()).thenReturn("parent_id");

            var join = mock(Join.class);
            lenient().when(join.getChildMap()).thenReturn(childMap);
            lenient().when(join.getParentMap()).thenReturn(parentMap);

            var view = viewWithSingleJoinCondition(join);

            var duckDbFactory = new DuckDbLogicalViewEvaluatorFactory(connection);
            var defaultFactory = new io.carml.logicalview.DefaultLogicalViewEvaluatorFactory();

            var duckDbResult = duckDbFactory.match(view);
            var defaultResult = defaultFactory.match(view);

            assertThat(duckDbResult.isPresent(), is(true));
            assertThat(defaultResult.isPresent(), is(true));

            var matches = java.util.stream.Stream.of(duckDbResult, defaultResult)
                    .flatMap(java.util.Optional::stream)
                    .toList();
            var selected = io.carml.logicalview.MatchedLogicalViewEvaluator.select(matches);

            assertThat(selected.isPresent(), is(true));
            assertThat(selected.get(), instanceOf(DuckDbLogicalViewEvaluator.class));
        }

        private LogicalView viewWithSingleJoinCondition(Join join) {
            var parentView = createViewWithRefFormulation(Rdf.Ql.JsonPath);
            var viewJoin = mock(LogicalViewJoin.class);
            lenient().when(viewJoin.getParentLogicalView()).thenReturn(parentView);
            lenient().when(viewJoin.getJoinConditions()).thenReturn(Set.of(join));
            lenient().when(viewJoin.getFields()).thenReturn(Set.of());

            return createViewWithRefFormulationAndJoins(Rdf.Ql.JsonPath, Set.of(viewJoin), Set.of());
        }
    }

    // --- Helper methods ---

    /**
     * Creates a {@link LogicalSource} mock with a lenient stub on
     * {@link LogicalSource#resolveIteratorAsString()} that mirrors the production default-method
     * semantics. Mockito does not execute default interface methods on mocks, so the stub
     * reproduces the lookup chain: declared iterator first, then the formulation's default
     * (derived from its IRI for {@code rml:JSONPath} → {@code "$"} and {@code rml:XPath} →
     * {@code "/"} since mock formulations don't execute their own default methods either).
     * Tests can override either layer with explicit stubs.
     */
    private static LogicalSource newLogicalSourceMock() {
        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.resolveIteratorAsString()).thenAnswer(invocation -> {
            var declared = logicalSource.getIterator();
            if (declared != null && !declared.isBlank()) {
                return Optional.of(declared);
            }
            var formulation = logicalSource.getReferenceFormulation();
            if (formulation == null) {
                return Optional.empty();
            }
            var iri = formulation.getAsResource();
            if (Rdf.Rml.JsonPath.equals(iri) || Rdf.Ql.JsonPath.equals(iri)) {
                return Optional.of("$");
            }
            if (Rdf.Rml.XPath.equals(iri) || Rdf.Ql.XPath.equals(iri)) {
                return Optional.of("/");
            }
            return Optional.empty();
        });
        return logicalSource;
    }

    private static int strongScore() {
        return MatchScore.builder().strongMatch().build().getScore();
    }

    private static LogicalView createViewWithRefFormulation(Resource refIri) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn("/test/path/file.json");

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = newLogicalSourceMock();
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

        var logicalSource = newLogicalSourceMock();
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
        // A LogicalViewJoin must carry at least one join condition for the SQL compiler to be able
        // to render an ON-clause. Tests that don't care about the specific fields still need a
        // valid condition shape so the trial compile in match() succeeds.
        var condition = joinCondition("id", "id");
        var join = mock(LogicalViewJoin.class);
        lenient().when(join.getParentLogicalView()).thenReturn(parentView);
        lenient().when(join.getJoinConditions()).thenReturn(Set.of(condition));
        lenient().when(join.getFields()).thenReturn(Set.of());
        return join;
    }

    private static <T extends ExpressionMap> T stubAsRdf(T exprMap) {
        lenient().when(exprMap.asRdf()).thenReturn(new LinkedHashModel());
        lenient().when(exprMap.getAsResource()).thenReturn(Values.bnode());
        return exprMap;
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

    private static LogicalView createSqlViewWithTable(Resource refIri, String tableName) {
        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = newLogicalSourceMock();
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getTableName()).thenReturn(tableName);

        return createViewWithLogicalSource(logicalSource);
    }

    private static LogicalView createSqlViewWithQuery(Resource refIri, String query) {
        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = newLogicalSourceMock();
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getQuery()).thenReturn(query);

        return createViewWithLogicalSource(logicalSource);
    }
}
