package io.carml.engine;

import java.util.List;

/**
 * Result of byte-mode mapping for a single iteration. Contains the encoded bytes for regular
 * triples/quads and any mergeable results that require cross-iteration handling.
 *
 * @param bytes the encoded N-Triples/N-Quads byte arrays
 * @param mergeables mergeable results to be collected and merged after all iterations
 */
public record ByteMappingResult<V>(List<byte[]> bytes, List<MappingResult<V>> mergeables) {}
