package io.carml.logicalview;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Flux;

class ExactDedupStrategy implements DedupStrategy {

    @Override
    public Flux<ViewIteration> deduplicate(Flux<ViewIteration> source, Set<String> keyFields) {
        var sortedKeys = keyFields.stream().sorted().toList();
        Set<List<Object>> seen = ConcurrentHashMap.newKeySet();
        return source.filter(iteration -> {
            var key = sortedKeys.stream()
                    .map(field -> iteration.getValue(field).orElse(null))
                    .toList();
            return seen.add(key);
        });
    }
}
