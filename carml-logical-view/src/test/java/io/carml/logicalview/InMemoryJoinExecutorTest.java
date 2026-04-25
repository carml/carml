package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.functions.FunctionRegistry;
import io.carml.model.ChildMap;
import io.carml.model.Join;
import io.carml.model.ParentMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

@ExtendWith(MockitoExtension.class)
class InMemoryJoinExecutorTest {

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
        return new DefaultViewIteration(index, values, Map.of(), Map.of());
    }

    @Test
    void matches_innerJoinOneToOne_returnsMatchedChildren() {
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.just(parent(0, Map.of("pid", "1", "#", 0)), parent(1, Map.of("pid", "2", "#", 1)));
        var children = Flux.just(
                child(Map.of("cid", "1", "#", 0)),
                child(Map.of("cid", "2", "#", 1)),
                child(Map.of("cid", "3", "#", 2)));

        try (var executor = new InMemoryJoinExecutor(new DefaultExpressionMapEvaluator(FunctionRegistry.create()))) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).child().values().get("cid"), is("1"));
            assertThat(rows.get(0).matchedParents(), hasSize(1));
            assertThat(rows.get(1).child().values().get("cid"), is("2"));
        }
    }

    @Test
    void matches_leftJoinNoMatch_emitsEmptyParentsList() {
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.<ViewIteration>empty();
        var children = Flux.just(child(Map.of("cid", "1", "#", 0)));

        try (var executor = new InMemoryJoinExecutor(new DefaultExpressionMapEvaluator(FunctionRegistry.create()))) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "#"), true)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).matchedParents(), is(empty()));
        }
    }

    @Test
    void matches_innerJoinNoMatch_filtersChildOut() {
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.just(parent(0, Map.of("pid", "1", "#", 0)));
        var children = Flux.just(child(Map.of("cid", "99", "#", 0)));

        try (var executor = new InMemoryJoinExecutor(new DefaultExpressionMapEvaluator(FunctionRegistry.create()))) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .collectList()
                    .block();

            assertThat(rows, is(empty()));
        }
    }

    @Test
    void matches_multipleParentsPerChild_returnsAllInOrder() {
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.just(
                parent(0, Map.of("pid", "1", "name", "A", "#", 0)),
                parent(1, Map.of("pid", "1", "name", "B", "#", 1)),
                parent(2, Map.of("pid", "1", "name", "C", "#", 2)));
        var children = Flux.just(child(Map.of("cid", "1", "#", 0)));

        try (var executor = new InMemoryJoinExecutor(new DefaultExpressionMapEvaluator(FunctionRegistry.create()))) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "name", "#"), false)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(1));
            var names = rows.get(0).matchedParents().stream()
                    .map(p -> p.getValue("name").orElseThrow())
                    .toList();
            assertThat(names, contains("A", "B", "C"));
        }
    }

    @Test
    void matches_multiConditionKey_usesAllComponents() {
        var conditions = List.of(joinCondition("c1", "p1"), joinCondition("c2", "p2"));
        var parents = Flux.just(
                parent(0, Map.of("p1", "a", "p2", "x", "#", 0)),
                parent(1, Map.of("p1", "a", "p2", "y", "#", 1)),
                parent(2, Map.of("p1", "b", "p2", "x", "#", 2)));
        var children =
                Flux.just(child(Map.of("c1", "a", "c2", "x", "#", 0)), child(Map.of("c1", "a", "c2", "y", "#", 1)));

        try (var executor = new InMemoryJoinExecutor(new DefaultExpressionMapEvaluator(FunctionRegistry.create()))) {
            var rows = executor.matches(parents, children, conditions, Set.of("p1", "p2", "#"), false)
                    .collectList()
                    .block();

            assertThat(rows, hasSize(2));
            var matchedIndices = rows.stream()
                    .flatMap(r -> r.matchedParents().stream())
                    .map(ViewIteration::getIndex)
                    .collect(Collectors.toSet());
            assertThat(matchedIndices, containsInAnyOrder(0, 1));
        }
    }

    @Test
    void matches_childKeyEmpty_filtersOrNullExtends() {
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.just(parent(0, Map.of("pid", "1", "#", 0)));
        // Child has no value for "cid" — key is empty.
        var children = Flux.just(child(Map.of("other", "v", "#", 0)));

        try (var inner = new InMemoryJoinExecutor(new DefaultExpressionMapEvaluator(FunctionRegistry.create()))) {
            var rows = inner.matches(parents, children, conditions, Set.of("pid", "#"), false)
                    .collectList()
                    .block();
            assertThat("INNER join with empty child key filters out the child", rows, is(empty()));
        }

        try (var left = new InMemoryJoinExecutor(new DefaultExpressionMapEvaluator(FunctionRegistry.create()))) {
            var rows = left.matches(
                            Flux.just(parent(0, Map.of("pid", "1", "#", 0))),
                            Flux.just(child(Map.of("other", "v", "#", 0))),
                            conditions,
                            Set.of("pid", "#"),
                            true)
                    .collectList()
                    .block();
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).matchedParents(), is(empty()));
        }
    }

    @Test
    void matches_parentKeyEmpty_isNotIndexedSoNoMatch() {
        // Parent is missing "pid" → parent-side key evaluation yields [].
        var conditions = List.of(joinCondition("cid", "pid"));
        var parents = Flux.just(parent(0, Map.of("other", "v", "#", 0)));
        var children = Flux.just(child(Map.of("cid", "1", "#", 0)));

        try (var executor = new InMemoryJoinExecutor(new DefaultExpressionMapEvaluator(FunctionRegistry.create()))) {
            var rows = executor.matches(parents, children, conditions, Set.of("pid", "other", "#"), false)
                    .collectList()
                    .block();
            assertThat(rows, is(empty()));
        }
    }

    @Test
    @SuppressWarnings("resource") // Test specifically verifies explicit close() idempotency,
    // try-with-resources would mask the contract under test.
    void close_idempotent_doesNotThrow() {
        var executor = new InMemoryJoinExecutor(new DefaultExpressionMapEvaluator(FunctionRegistry.create()));
        assertDoesNotThrow(() -> {
            executor.close();
            executor.close();
        });
    }

    @Test
    void factory_createReturnsFreshInstance() {
        var factory = new InMemoryJoinExecutorFactory();
        var evaluator = new DefaultExpressionMapEvaluator(FunctionRegistry.create());
        try (var first = factory.create(evaluator);
                var second = factory.create(evaluator)) {
            assertThat(first, is(notNullValue()));
            assertThat(second, is(notNullValue()));
            // Different instances — no shared state.
            assertThat(first == second, is(false));
        }
    }
}
