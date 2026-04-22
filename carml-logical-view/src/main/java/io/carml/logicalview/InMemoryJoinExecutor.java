package io.carml.logicalview;

import io.carml.model.Join;
import java.util.List;
import java.util.Set;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Default {@link JoinExecutor} that drains parents into a {@link HashMapJoinIndex} probe table,
 * then emits one {@link MatchedRow} per child by looking up the child's join key. Same memory
 * profile as the pre-Task-6.41 evaluator path — the entire parent stream is materialized in heap.
 *
 * <p>Used when the user has not opted in to spillable joins. For large parent streams that exceed
 * heap, see {@code DuckDbJoinExecutor} in the {@code carml-logical-view-join-duckdb} module.
 */
public final class InMemoryJoinExecutor implements JoinExecutor {

    @Override
    public Flux<MatchedRow> matches(
            Flux<ViewIteration> parents,
            Flux<EvaluatedValues> children,
            List<Join> conditions,
            Set<String> parentReferenceableKeys,
            boolean leftJoin) {

        // Flux.using owns the index lifecycle: built lazily on subscribe, closed on terminate /
        // cancel / error. HashMapJoinIndex#close is a no-op today but JoinIndex extends
        // AutoCloseable for disk-backed implementations, so cleanup must be wired through here too.
        return parents.collectList()
                .flatMapMany(parentList -> Flux.using(
                        () -> buildIndex(parentList, conditions, parentReferenceableKeys),
                        index -> children.flatMap(child -> matchChild(child, conditions, leftJoin, index)),
                        JoinIndex::close));
    }

    private static HashMapJoinIndex<List<Object>, ViewIteration> buildIndex(
            List<ViewIteration> parents, List<Join> conditions, Set<String> parentReferenceableKeys) {
        var index = new HashMapJoinIndex<List<Object>, ViewIteration>();
        parents.forEach(parent -> {
            var key = JoinKeyExtractor.parentKey(conditions, parent, parentReferenceableKeys);
            if (!key.isEmpty()) {
                index.put(key, parent);
            }
        });
        return index;
    }

    private static Mono<MatchedRow> matchChild(
            EvaluatedValues child,
            List<Join> conditions,
            boolean leftJoin,
            JoinIndex<List<Object>, ViewIteration> index) {
        var ckey = JoinKeyExtractor.childKey(conditions, child);
        var matched = ckey.isEmpty() ? List.<ViewIteration>of() : index.get(ckey);
        if (matched.isEmpty() && !leftJoin) {
            return Mono.empty();
        }
        return Mono.just(new MatchedRow(child, matched));
    }
}
