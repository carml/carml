package com.taxonic.carml.engine;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.TriplesMap;
import java.io.InputStream;
import java.util.Map;
import reactor.core.publisher.Flux;

public interface LogicalSourcePipeline<I, V> {

  LogicalSource getLogicalSource();

  Map<TriplesMap, Flux<V>> run();

  Map<TriplesMap, Flux<V>> run(InputStream inputStream);

}
