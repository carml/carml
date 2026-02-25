package io.carml.logicalview;

import java.util.Set;
import reactor.core.publisher.Flux;

class NoneDedupStrategy implements DedupStrategy {

    @Override
    public Flux<ViewIteration> deduplicate(Flux<ViewIteration> source, Set<String> keyFields) {
        return source;
    }
}
