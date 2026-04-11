package io.carml.output;

/**
 * Service Provider Interface (SPI) for creating {@link RdfSerializer} instances. Implementations
 * are discovered at runtime via {@link java.util.ServiceLoader} and selected by
 * {@code RdfSerializerFactory} based on format support and priority.
 *
 * <p>Multiple providers may support the same format and mode combination. The factory selects the
 * provider with the highest {@link #priority()} value, allowing higher-performance or
 * feature-richer implementations to override baseline ones.
 *
 * <p>Recommended priority ranges:
 * <ul>
 *   <li><strong>100</strong> — Fast byte-level serializers (FastNT/NQ), for N-Triples/N-Quads only</li>
 *   <li><strong>50</strong> — Jena-based serializers, broad format support</li>
 *   <li><strong>10</strong> — RDF4J Rio-based serializers, baseline fallback</li>
 * </ul>
 *
 * <p>Implementations must be registered for {@link java.util.ServiceLoader} discovery, typically
 * via {@code @AutoService(RdfSerializerProvider.class)}.
 *
 * <p>Format strings are file-extension-based identifiers (e.g. {@code "nt"}, {@code "nq"},
 * {@code "ttl"}, {@code "trig"}, {@code "jsonld"}, {@code "rdf"}, {@code "n3"}).
 *
 * @see RdfSerializer
 * @see SerializerMode
 */
public interface RdfSerializerProvider {

    /**
     * Returns whether this provider can create a serializer for the given format and mode.
     *
     * @param format the RDF format identifier (file extension, e.g. {@code "nt"}, {@code "ttl"})
     * @param mode the requested serialization mode
     * @return {@code true} if this provider supports the combination
     */
    boolean supports(String format, SerializerMode mode);

    /**
     * Returns the priority of this provider. When multiple providers support the same format and
     * mode, the one with the highest priority is selected.
     *
     * @return a non-negative priority value; higher means preferred
     */
    int priority();

    /**
     * Creates a new {@link RdfSerializer} for the given format and mode. The caller must invoke
     * {@link RdfSerializer#start} before writing statements.
     *
     * <p>This method must only be called after verifying that {@link #supports(String, SerializerMode)}
     * returns {@code true} for the same arguments.
     *
     * @param format the RDF format identifier (file extension, e.g. {@code "nt"}, {@code "ttl"})
     * @param mode the requested serialization mode
     * @return a new serializer instance, never null
     * @throws IllegalArgumentException if the format/mode combination is not supported
     */
    RdfSerializer createSerializer(String format, SerializerMode mode);
}
