package io.carml.logicalview;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Flux;

class SimpleEqualityDedupStrategy implements DedupStrategy {

    @Override
    public Flux<ViewIteration> deduplicate(Flux<ViewIteration> source, Set<String> keyFields) {
        var sortedKeys = keyFields.stream().sorted().toList();
        Set<List<Object>> seen = ConcurrentHashMap.newKeySet();
        return source.filter(iteration -> {
            var key = sortedKeys.stream()
                    .map(field -> iteration
                            .getValue(field)
                            .orElseThrow(() -> new IllegalStateException(
                                    ("SimpleEqualityDedupStrategy encountered null for field '%s';"
                                                    + " this strategy requires all key fields to be non-null")
                                            .formatted(field))))
                    .toList();
            return seen.add(key);
        });
    }
}
