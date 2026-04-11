package io.carml.output;

/**
 * Specifies the serialization mode used when creating an {@link RdfSerializer} via an
 * {@link RdfSerializerProvider}.
 *
 * <p>The mode affects both the serialization strategy and the output quality:
 * <ul>
 *   <li>{@link #STREAMING} — write statements one-by-one without buffering the full model.
 *       Suitable for large datasets and streaming pipelines. Output may lack pretty-printing
 *       features like prefix-based shortening.</li>
 *   <li>{@link #PRETTY} — buffer the full model in memory before writing, enabling
 *       pretty-printing with prefix shortening, blank node inlining, and sorted output.
 *       Not suitable for very large datasets.</li>
 *   <li>{@link #BYTE_LEVEL} — high-throughput byte-level encoding where each statement is
 *       independently serialized to a {@code byte[]}. Only applicable to line-based formats
 *       (N-Triples, N-Quads). Enables the {@link RdfSerializer#encode(org.eclipse.rdf4j.model.Statement)}
 *       fast path.</li>
 * </ul>
 */
public enum SerializerMode {

    /**
     * Statement-by-statement streaming serialization. The serializer writes each statement
     * directly to the output stream without buffering the full model.
     */
    STREAMING,

    /**
     * Pretty-printed serialization. The serializer may buffer the full model to produce
     * human-readable output with prefix shortening and blank node inlining.
     */
    PRETTY,

    /**
     * Byte-level encoding for maximum throughput. Each statement can be independently encoded
     * to a {@code byte[]} array via {@link RdfSerializer#encode(org.eclipse.rdf4j.model.Statement)}.
     * Only supported by line-based formats (N-Triples, N-Quads).
     */
    BYTE_LEVEL
}
