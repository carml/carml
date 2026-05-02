package io.carml.engine;

import java.util.List;
import reactor.core.publisher.Flux;

/**
 * Result of byte-mode mapping for a single iteration. Contains a {@link Flux} of encoded bytes for
 * regular triples/quads and any mergeable results that require cross-iteration handling.
 *
 * <p>The bytes flux is lazy — subscribing pulls one batch (or one byte array) at a time from the
 * underlying mapping pipeline. This bounds per-iteration heap retention to the working-set size,
 * not the total per-iteration cartesian-product cardinality.
 *
 * @param bytes a flux of encoded N-Triples/N-Quads byte arrays
 * @param mergeables mergeable results to be collected and merged after all iterations
 */
public record ByteMappingResult<V>(Flux<byte[]> bytes, List<MappingResult<V>> mergeables) {}
