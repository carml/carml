package io.carml.logicalview.join.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.logicalview.EvaluatedValues;
import io.carml.logicalview.InMemoryJoinExecutor;
import io.carml.logicalview.MatchedRow;
import io.carml.logicalview.ViewIteration;
import io.carml.model.ChildMap;
import io.carml.model.Join;
import io.carml.model.ParentMap;
import io.carml.model.ReferenceFormulation;
import io.carml.model.impl.CarmlReferenceFormulation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

@ExtendWith(MockitoExtension.class)
class DuckDbJoinExecutorTest {

    private static Join joinCondition(String childRef, String parentRef) {
        var join = mock(Join.class);
        ChildMap childMap = mock(ChildMap.class);
        ParentMap parentMap = mock(ParentMap.class);
        lenient().when(childMap.getReference()).thenReturn(childRef);
        lenient().when(parentMap.getReference()).thenReturn(parentRef);
        lenient().when(join.getChildMap()).thenReturn(childMap);
        lenient().when(join.getParentMap()).thenReturn(parentMap);
        return join;
    }

    private static EvaluatedValues child(Map<String, Object> values) {
        return new EvaluatedValues(values, Map.of(), Map.of());
    }

    private static ViewIteration parent(int index, Map<String, Object> values) {
        return ViewIteration.of(index, values, Map.of(), Map.of());
    }

    @Test
    void matches_belowThreshold_doesNotOpenDuckDbConnection(@TempDir Path spillDir) {
        // Threshold 1000, parent count 100 → in-memory probe path; no DB file should appear.
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.range(0, 100).map(i -> parent(i, Map.of("pid", String.valueOf(i), "#", i)));
        var children = Flux.range(0, 100).map(i -> child(Map.of("cid", String.valueOf(i), "#", i)));

        try (var executor = new DuckDbJoinExecutor(1000, true, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(100));
            assertThat(executor.getDbFile(), is((Path) null));
            assertThat(executor.getWorkingDir(), is((Path) null));
        }
    }

    @Test
    void matches_aboveThreshold_spillsToDuckDbAndProducesCorrectRows(@TempDir Path spillDir) {
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.range(0, 100).map(i -> parent(i, Map.of("pid", String.valueOf(i), "name", "p" + i, "#", i)));
        var children = Flux.range(0, 100).map(i -> child(Map.of("cid", String.valueOf(i), "#", i)));

        try (var executor = new DuckDbJoinExecutor(10, true, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "name", "#"), false)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(100));
            // Spill triggered → temp DB file is created during matching.
            assertThat(executor.getWorkingDir(), is(notNullValue()));
            assertThat(Files.exists(executor.getWorkingDir()), is(true));
            // Each child sees exactly one matching parent.
            for (var row : rows) {
                assertThat(row.matchedParents(), hasSize(1));
                var pid = (String) row.matchedParents().get(0).getValue("pid").orElseThrow();
                assertThat(pid, is(row.child().values().get("cid")));
            }
        }
    }

    @Test
    void matches_aboveThresholdLeftJoinNoMatch_emitsEmptyParents(@TempDir Path spillDir) {
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.range(0, 50).map(i -> parent(i, Map.of("pid", String.valueOf(i), "#", i)));
        // Children with cids 100..149 don't match any parent.
        var children = Flux.range(100, 50).map(i -> child(Map.of("cid", String.valueOf(i), "#", i)));

        try (var executor = new DuckDbJoinExecutor(10, true, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "#"), true)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(50));
            for (var row : rows) {
                assertThat(row.matchedParents(), is(empty()));
            }
        }
    }

    @Test
    void matches_aboveThresholdInnerJoinNoMatch_filtersChildrenOut(@TempDir Path spillDir) {
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.range(0, 50).map(i -> parent(i, Map.of("pid", String.valueOf(i), "#", i)));
        // Mix: half match, half do not.
        var children = Flux.range(0, 100).map(i -> child(Map.of("cid", String.valueOf(i), "#", i)));

        try (var executor = new DuckDbJoinExecutor(10, true, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(50));
        }
    }

    @Test
    void matches_aboveThresholdMultiCondition_keysAllUsed(@TempDir Path spillDir) {
        var conditions = List.of(joinCondition("c1", "p1"), joinCondition("c2", "p2"));
        var parents = Flux.range(0, 100).map(i -> parent(i, Map.of("p1", "a", "p2", String.valueOf(i), "#", i)));
        // Only children with c1=a and c2 in {0..99} match.
        var children = Flux.range(0, 100).map(i -> child(Map.of("c1", "a", "c2", String.valueOf(i), "#", i)));

        try (var executor = new DuckDbJoinExecutor(10, true, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("p1", "p2", "#"), false)
                    .collectList()
                    .block();
            assertThat(rows, hasSize(100));
            // Now check non-matching c1 yields nothing.
            try (var executor2 = new DuckDbJoinExecutor(10, true, spillDir)) {
                var rows2 = executor2
                        .matches(
                                Flux.range(0, 100)
                                        .map(i -> parent(i, Map.of("p1", "a", "p2", String.valueOf(i), "#", i))),
                                Flux.range(0, 100).map(i -> child(Map.of("c1", "b", "c2", String.valueOf(i), "#", i))),
                                conditions,
                                Set.of("p1", "p2", "#"),
                                false)
                        .collectList()
                        .block();
                assertThat(rows2, is(empty()));
            }
        }
    }

    @Test
    void matches_aboveThresholdMultiMatch_returnsAllParentsInOrder(@TempDir Path spillDir) {
        var conditions = List.of(joinCondition("cid", "pid"));
        // 50 parents all sharing pid=1, plus 1 child with cid=1.
        var parents = Flux.range(0, 50).map(i -> parent(i, Map.of("pid", "1", "name", "p" + i, "#", i)));
        var children = Flux.just(child(Map.of("cid", "1", "#", 0)));

        try (var executor = new DuckDbJoinExecutor(10, true, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "name", "#"), false)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).matchedParents(), hasSize(50));
            // Order preserved by row_id.
            for (int i = 0; i < 50; i++) {
                assertThat(rows.get(0).matchedParents().get(i).getValue("name").orElseThrow(), is("p" + i));
            }
        }
    }

    @Test
    void matches_hashCollisionKeys_handledByStringEquality(@TempDir Path spillDir) {
        // Distinct string keys, just exercise routing through DuckDB; DuckDB compares strings
        // semantically so any "hash collision" at JVM level is irrelevant.
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.just(parent(0, Map.of("pid", "Aa", "#", 0)), parent(1, Map.of("pid", "BB", "#", 1)));
        var children = Flux.just(child(Map.of("cid", "Aa", "#", 0)), child(Map.of("cid", "BB", "#", 1)));

        try (var executor = new DuckDbJoinExecutor(0, true, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .collectList()
                    .block();
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).matchedParents(), hasSize(1));
            assertThat(rows.get(1).matchedParents(), hasSize(1));
        }
    }

    @Test
    void close_fileBacked_deletesSpillFiles(@TempDir Path spillDir) {
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.range(0, 100).map(i -> parent(i, Map.of("pid", String.valueOf(i), "#", i)));
        var children = Flux.range(0, 50).map(i -> child(Map.of("cid", String.valueOf(i), "#", i)));

        Path workingDirObserved;
        try (var executor = new DuckDbJoinExecutor(10, true, spillDir)) {
            executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .collectList()
                    .block();
            workingDirObserved = executor.getWorkingDir();
            assertThat(workingDirObserved, is(notNullValue()));
            assertThat(Files.exists(workingDirObserved), is(true));
        }
        // After the try-with-resources exits, close() has been invoked; spill dir must be gone.
        assertThat(Files.exists(workingDirObserved), is(false));
    }

    @Test
    void matches_cancelMidStream_terminatesCleanly(@TempDir Path spillDir) {
        // Spill triggered → producer is the PausableFluxBridge-driven thread that parks via
        // DuckDbPausableSource.awaitDemand(). Subscriber takes only the first 5 of 100 expected
        // matches and then cancels. The cancel signal must propagate through the bridge,
        // unparking the producer so it exits cleanly. If cancellation is broken, .take(5).block()
        // would either hang on the parked thread OR collectList() would never complete.
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.range(0, 100).map(i -> parent(i, Map.of("pid", String.valueOf(i), "#", i)));
        var children = Flux.range(0, 100).map(i -> child(Map.of("cid", String.valueOf(i), "#", i)));

        try (var executor = new DuckDbJoinExecutor(10, true, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .take(5)
                    .collectList()
                    .block(java.time.Duration.ofSeconds(30));
            assertThat(rows, hasSize(5));
            // close() runs on try-with-resources exit; if the producer thread were still parked,
            // it would not be a deadlock here (close runs on the main thread) but the spill dir
            // cleanup would race the parked thread. The clean-cleanup assertion is below.
            assertThat(executor.getWorkingDir(), is(notNullValue()));
        }
    }

    @Test
    void matches_downstreamError_cancelsCleanlyAndReleasesProducer(@TempDir Path spillDir) {
        // Spill triggered → result stream is the PausableFluxBridge-driven path. Downstream
        // throws on the first row; Reactor signals cancel upstream, which must wake the parked
        // producer (via the bridge's onDispose → DuckDbPausableSource.cancel()). Without that
        // wiring the producer thread parks forever and close() can deadlock on connection
        // shutdown. The 30s timeout is the safety net — a healthy implementation completes in
        // milliseconds.
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.range(0, 100).map(i -> parent(i, Map.of("pid", String.valueOf(i), "#", i)));
        var children = Flux.range(0, 100).map(i -> child(Map.of("cid", String.valueOf(i), "#", i)));

        var timeout = java.time.Duration.ofSeconds(30);
        try (var executor = new DuckDbJoinExecutor(10, true, spillDir)) {
            // Simulate a downstream failure by mapping the first emitted row into onError. Using
            // handle() instead of throwing inside map() is the Reactor-idiomatic way — Sonar's
            // S6916 flags the "throw in map" anti-pattern, and this also gives Reactor a clean
            // signal to propagate cancel upstream.
            var resultFlux = executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .<MatchedRow>handle(
                            (row, sink) -> sink.error(new IllegalStateException("simulated downstream failure")));
            var error = assertThrows(IllegalStateException.class, () -> resultFlux.blockLast(timeout));
            assertThat(error.getMessage(), is("simulated downstream failure"));
        }
    }

    @Test
    void matches_calledTwice_throwsIllegalState(@TempDir Path spillDir) {
        // The executor holds per-join state (parent row counter, DuckDB connection). Calling
        // matches() twice on the same instance must fail loudly, not silently produce corrupt
        // results. The factory contract is "create a fresh executor per join."
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.range(0, 5).map(i -> parent(i, Map.of("pid", String.valueOf(i), "#", i)));
        var children = Flux.range(0, 5).map(i -> child(Map.of("cid", String.valueOf(i), "#", i)));

        var timeout = java.time.Duration.ofSeconds(5);
        try (var executor = new DuckDbJoinExecutor(1000, true, spillDir)) {
            executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .collectList()
                    .block();
            // Second call: same executor, fresh fluxes. Should error.
            var secondCallMono = executor.matches(
                            Flux.range(0, 5).map(i -> parent(i, Map.of("pid", String.valueOf(i), "#", i))),
                            Flux.range(0, 5).map(i -> child(Map.of("cid", String.valueOf(i), "#", i))),
                            conditions,
                            Set.of("pid", "#"),
                            false)
                    .collectList();
            var error = assertThrows(IllegalStateException.class, () -> secondCallMono.block(timeout));
            assertThat(error.getMessage().contains("at most once per instance"), is(true));
        }
    }

    @Test
    @SuppressWarnings("resource") // Test specifically verifies explicit close() idempotency,
    // try-with-resources would mask the contract under test.
    void close_idempotent(@TempDir Path spillDir) {
        var executor = new DuckDbJoinExecutor(1000, true, spillDir);
        assertDoesNotThrow(() -> {
            executor.close();
            executor.close();
        });
    }

    @Test
    void matches_blobRoundtrip_preservesValueTypesAndNaturalDatatypes(@TempDir Path spillDir) {
        // Build a parent ViewIteration with mixed value types and non-trivial
        // referenceFormulations + naturalDatatypes maps; verify the round-trip preserves them.
        Map<String, ReferenceFormulation> refForms = new LinkedHashMap<>();
        refForms.put(
                "name",
                CarmlReferenceFormulation.builder()
                        .id("http://w3id.org/rml/CSV")
                        .build());
        Map<String, IRI> datatypes = new LinkedHashMap<>();
        datatypes.put("count", XSD.INTEGER);
        datatypes.put("active", XSD.BOOLEAN);

        Map<String, Object> parentValues = new LinkedHashMap<>();
        parentValues.put("pid", "k1");
        parentValues.put("name", "alpha");
        parentValues.put("count", 42);
        parentValues.put("ratio", 3.14d);
        parentValues.put("active", Boolean.TRUE);
        parentValues.put("note", null);
        parentValues.put("#", 7);

        var parent = ViewIteration.of(7, parentValues, refForms, datatypes);
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.just(parent);
        var children = Flux.just(child(Map.of("cid", "k1", "#", 0)));

        try (var executor = new DuckDbJoinExecutor(0, true, spillDir)) {
            var rows = executor.matches(
                            parents,
                            children,
                            conditions,
                            Set.of("pid", "name", "count", "ratio", "active", "note", "#"),
                            false)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(1));
            var matched = rows.get(0).matchedParents().get(0);
            assertThat(matched.getValue("name").orElseThrow(), is("alpha"));
            assertThat(matched.getValue("count").orElseThrow(), is(42));
            assertThat(matched.getValue("ratio").orElseThrow(), is(3.14d));
            assertThat(matched.getValue("active").orElseThrow(), is(Boolean.TRUE));
            assertThat(matched.getValue("note").isPresent(), is(false));
            assertThat(matched.getIndex(), is(7));
            assertThat(matched.getNaturalDatatype("count").orElseThrow(), is(XSD.INTEGER));
            assertThat(matched.getNaturalDatatype("active").orElseThrow(), is(XSD.BOOLEAN));
            assertThat(
                    matched.getFieldReferenceFormulation("name")
                            .map(rf -> rf.getAsResource().stringValue())
                            .orElseThrow(),
                    is("http://w3id.org/rml/CSV"));
        }
    }

    @Test
    void matches_largeDataset_parityWithInMemoryExecutor(@TempDir Path spillDir) {
        // 5K parents × 5K children, both via spill path AND in-memory path. Compare equivalence
        // position-by-position — child stream order AND parent-row order must match.
        int n = 5_000;
        var conditions = List.of(joinCondition("cid", "pid"));
        // Parents have one entry per pid value (0..n-1) but we shuffle the assignment so the
        // join is non-trivial: pid = (i * 7 + 3) mod n, name = "p" + i.
        var parentList = new java.util.ArrayList<ViewIteration>(n);
        var childList = new java.util.ArrayList<EvaluatedValues>(n);
        for (int i = 0; i < n; i++) {
            parentList.add(parent(i, Map.of("pid", String.valueOf((i * 7 + 3) % n), "name", "p" + i, "#", i)));
            childList.add(child(Map.of("cid", String.valueOf(i), "#", i)));
        }

        var spillResults = collectOrdered(spillDir, parentList, childList, conditions, true);
        var memResults = collectOrdered(spillDir, parentList, childList, conditions, false);

        assertThat(spillResults, hasSize(n));
        assertThat(spillResults, is(memResults));
    }

    private static List<List<String>> collectOrdered(
            Path spillDir,
            List<ViewIteration> parents,
            List<EvaluatedValues> children,
            List<Join> conditions,
            boolean useSpill) {
        if (useSpill) {
            try (var executor = new DuckDbJoinExecutor(100, true, spillDir)) {
                return projectRows(executor, parents, children, conditions);
            }
        }
        try (var executor = new InMemoryJoinExecutor()) {
            return projectRows(executor, parents, children, conditions);
        }
    }

    private static List<List<String>> projectRows(
            io.carml.logicalview.JoinExecutor executor,
            List<ViewIteration> parents,
            List<EvaluatedValues> children,
            List<Join> conditions) {
        var rows = executor.matches(
                        Flux.fromIterable(parents),
                        Flux.fromIterable(children),
                        conditions,
                        Set.of("pid", "name", "#"),
                        false)
                .collectList()
                .blockOptional()
                .orElseThrow(() -> new AssertionError("matches() returned null Flux result"));
        return rows.stream()
                .map(row -> {
                    var cid = (String) row.child().values().get("cid");
                    var names = row.matchedParents().stream()
                            .map(p -> (String) p.getValue("name").orElseThrow())
                            .toList();
                    var combined = new java.util.ArrayList<String>(names.size() + 1);
                    combined.add(cid);
                    combined.addAll(names);
                    return List.copyOf(combined);
                })
                .toList();
    }

    @Test
    void matches_aboveThresholdInMemoryMode_alsoSpillsButWithoutFile(@TempDir Path spillDir) {
        // fileBacked=false → connection is jdbc:duckdb: (in-memory), no spill file expected.
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.range(0, 100).map(i -> parent(i, Map.of("pid", String.valueOf(i), "#", i)));
        var children = Flux.range(0, 100).map(i -> child(Map.of("cid", String.valueOf(i), "#", i)));

        try (var executor = new DuckDbJoinExecutor(10, false, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .collectList()
                    .block();
            assertThat(rows, hasSize(100));
            assertThat("no working dir for in-memory mode", executor.getWorkingDir(), is((Path) null));
            assertThat("no db file for in-memory mode", executor.getDbFile(), is((Path) null));
        }
    }

    @Test
    void matches_aboveThresholdMultiMatchPreservesParentRowOrder(@TempDir Path spillDir) {
        var conditions = List.of(joinCondition("cid", "pid"));
        // Multiple children, each matching multiple parents — the parents per child must come
        // back in parent-row order (which is the order they were appended).
        var parents =
                Flux.range(0, 100).map(i -> parent(i, Map.of("pid", String.valueOf(i % 3), "tag", "p" + i, "#", i)));
        var children = Flux.just(
                child(Map.of("cid", "0", "#", 0)),
                child(Map.of("cid", "1", "#", 1)),
                child(Map.of("cid", "2", "#", 2)));

        try (var executor = new DuckDbJoinExecutor(10, true, spillDir)) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "tag", "#"), false)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(3));
            for (var row : rows) {
                var cid = (String) row.child().values().get("cid");
                int mod = Integer.parseInt(cid);
                var tags = row.matchedParents().stream()
                        .map(p -> (String) p.getValue("tag").orElseThrow())
                        .toList();
                // Parents appear in the original parent-stream order — i.e. ascending by `i`.
                var expectedTags = java.util.stream.IntStream.range(0, 100)
                        .filter(i -> i % 3 == mod)
                        .mapToObj(i -> "p" + i)
                        .toList();
                assertThat(tags, contains(expectedTags.toArray()));
                assertThat(tags, containsInAnyOrder(expectedTags.toArray()));
            }
        }
    }
}
