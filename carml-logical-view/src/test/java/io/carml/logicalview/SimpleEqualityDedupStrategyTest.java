package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class SimpleEqualityDedupStrategyTest {

    private final DedupStrategy strategy = DedupStrategy.simpleEquality();

    private ViewIteration iteration(int index, String key, Object value) {
        return new DefaultViewIteration(index, Map.of(key, value), Map.of(), Map.of());
    }

    private ViewIteration iteration(int index, String key1, Object value1, String key2, Object value2) {
        return new DefaultViewIteration(index, Map.of(key1, value1, key2, value2), Map.of(), Map.of());
    }

    @Test
    void givenDuplicateRows_whenDeduplicated_thenOneEmitted() {
        var source = Flux.just(iteration(0, "name", "alice"), iteration(1, "name", "alice"));

        var result = strategy.deduplicate(source, Set.of("name")).collectList().block();

        assertThat(result, hasSize(1));
        assertThat(result.get(0).getValue("name"), is(Optional.of("alice")));
    }

    @Test
    void givenDistinctRows_whenDeduplicated_thenBothEmitted() {
        var source = Flux.just(iteration(0, "name", "alice"), iteration(1, "name", "bob"));

        var result = strategy.deduplicate(source, Set.of("name")).collectList().block();

        assertThat(result, hasSize(2));
    }

    @Test
    void givenCompositeKey_whenDeduplicated_thenDeduplicatesOnCombination() {
        var source = Flux.just(
                iteration(0, "a", "x", "b", "1"), iteration(1, "a", "x", "b", "2"), iteration(2, "a", "x", "b", "1"));

        var result =
                strategy.deduplicate(source, Set.of("a", "b")).collectList().block();

        assertThat(result, hasSize(2));
    }
}
