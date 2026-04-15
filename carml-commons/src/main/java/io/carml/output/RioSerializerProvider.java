package io.carml.output;

import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.rio.RDFFormat;

/**
 * {@link RdfSerializerProvider} backed by RDF4J Rio writers. Serves as the baseline fallback
 * for RDF serialization across a broad set of formats (N-Triples, N-Quads, Turtle, TriG, RDF/XML,
 * JSON-LD, N3).
 *
 * <p>Registered at priority 10, so higher-priority providers (e.g. {@link FastSerializerProvider}
 * at priority 100) override it for formats where a specialized implementation exists.
 *
 * <p>Supports {@link SerializerMode#STREAMING} (via {@link RioStreamingSerializer}) and
 * {@link SerializerMode#PRETTY} (via {@link RioModelSerializer}). {@link SerializerMode#BYTE_LEVEL}
 * is always unsupported — byte-level encoding is owned by specialized Fast serializers.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}.
 */
@AutoService(RdfSerializerProvider.class)
public final class RioSerializerProvider implements RdfSerializerProvider {

    private static final Map<String, RDFFormat> FORMAT_BY_ALIAS = Map.ofEntries(
            Map.entry("nt", RDFFormat.NTRIPLES),
            Map.entry("ntriples", RDFFormat.NTRIPLES),
            Map.entry("nq", RDFFormat.NQUADS),
            Map.entry("nquads", RDFFormat.NQUADS),
            Map.entry("ttl", RDFFormat.TURTLE),
            Map.entry("turtle", RDFFormat.TURTLE),
            Map.entry("trig", RDFFormat.TRIG),
            Map.entry("rdf", RDFFormat.RDFXML),
            Map.entry("rdfxml", RDFFormat.RDFXML),
            Map.entry("jsonld", RDFFormat.JSONLD),
            Map.entry("n3", RDFFormat.N3));

    private static final Set<SerializerMode> SUPPORTED_MODES = Set.of(SerializerMode.STREAMING, SerializerMode.PRETTY);

    @Override
    public boolean supports(String format, SerializerMode mode) {
        if (format == null || mode == null) {
            return false;
        }
        return FORMAT_BY_ALIAS.containsKey(format) && SUPPORTED_MODES.contains(mode);
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public RdfSerializer createSerializer(String format, SerializerMode mode) {
        if (!supports(format, mode)) {
            throw new IllegalArgumentException("Unsupported format/mode combination: %s/%s".formatted(format, mode));
        }
        var rdfFormat = FORMAT_BY_ALIAS.get(format);
        return switch (mode) {
            case STREAMING -> new RioStreamingSerializer(rdfFormat);
            case PRETTY -> new RioModelSerializer(rdfFormat);
            case BYTE_LEVEL -> throw new AssertionError("unreachable — supports() rejects BYTE_LEVEL");
        };
    }
}
