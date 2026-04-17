package io.carml.output.jena;

import com.google.auto.service.AutoService;
import io.carml.output.RdfSerializer;
import io.carml.output.RdfSerializerProvider;
import io.carml.output.SerializerMode;
import java.util.Map;
import java.util.Set;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDFWriter;

/**
 * {@link RdfSerializerProvider} backed by Apache Jena. Provides serialization across a broad set
 * of RDF formats using Jena's RIOT framework, serving as a middle-tier provider between the
 * high-throughput Fast serializers and the baseline Rio fallback.
 *
 * <p>Registered at priority 50, so it overrides the Rio provider (priority 10) but defers to
 * the Fast serializers (priority 100) for N-Triples/N-Quads {@link SerializerMode#BYTE_LEVEL}
 * and {@link SerializerMode#STREAMING} modes.
 *
 * <p>Supports {@link SerializerMode#STREAMING} (via {@link JenaStreamingSerializer} backed by
 * Jena's {@link org.apache.jena.riot.system.StreamRDF}) and {@link SerializerMode#PRETTY} (via
 * {@link JenaModelSerializer} backed by {@link org.apache.jena.riot.RDFDataMgr}).
 * {@link SerializerMode#BYTE_LEVEL} is always unsupported.
 *
 * <p>For {@link SerializerMode#STREAMING}, only formats with a registered
 * {@link StreamRDFWriter} are supported (typically N-Triples, N-Quads, Turtle, TriG). For
 * {@link SerializerMode#PRETTY}, all formats are supported.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}.
 */
@AutoService(RdfSerializerProvider.class)
public final class JenaSerializerProvider implements RdfSerializerProvider {

    private static final Map<String, Lang> FORMAT_BY_ALIAS = Map.ofEntries(
            Map.entry("nt", Lang.NTRIPLES),
            Map.entry("ntriples", Lang.NTRIPLES),
            Map.entry("nq", Lang.NQUADS),
            Map.entry("nquads", Lang.NQUADS),
            Map.entry("ttl", Lang.TURTLE),
            Map.entry("turtle", Lang.TURTLE),
            Map.entry("trig", Lang.TRIG),
            Map.entry("rdf", Lang.RDFXML),
            Map.entry("rdfxml", Lang.RDFXML),
            Map.entry("jsonld", Lang.JSONLD),
            Map.entry("n3", Lang.N3));

    private static final Set<SerializerMode> PRETTY_ONLY = Set.of(SerializerMode.PRETTY);

    private static final Set<SerializerMode> STREAMING_AND_PRETTY =
            Set.of(SerializerMode.STREAMING, SerializerMode.PRETTY);

    @Override
    public boolean supports(String format, SerializerMode mode) {
        if (format == null || mode == null) {
            return false;
        }
        var lang = FORMAT_BY_ALIAS.get(format);
        if (lang == null) {
            return false;
        }
        return supportedModes(lang).contains(mode);
    }

    @Override
    public int priority() {
        return 50;
    }

    @Override
    public RdfSerializer createSerializer(String format, SerializerMode mode) {
        if (!supports(format, mode)) {
            throw new IllegalArgumentException("Unsupported format/mode combination: %s/%s".formatted(format, mode));
        }
        var lang = FORMAT_BY_ALIAS.get(format);
        return switch (mode) {
            case STREAMING -> new JenaStreamingSerializer(lang);
            case PRETTY -> new JenaModelSerializer(lang);
            case BYTE_LEVEL -> throw new AssertionError("unreachable - supports() rejects BYTE_LEVEL");
        };
    }

    private static Set<SerializerMode> supportedModes(Lang lang) {
        return StreamRDFWriter.registered(lang) ? STREAMING_AND_PRETTY : PRETTY_ONLY;
    }
}
