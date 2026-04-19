package io.carml.output.jena;

import io.carml.output.RdfSerializationException;
import io.carml.output.RdfSerializer;
import io.carml.util.jena.JenaConverters;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RIOT;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.util.Context;
import org.eclipse.rdf4j.model.Statement;

/**
 * Streaming {@link RdfSerializer} backed by Apache Jena's {@link StreamRDF} writer.
 *
 * <p>Writes statements one-by-one without buffering the full model. For triples-only formats
 * (Turtle, N-Triples, RDF/XML, N3) each statement is emitted as a triple; for quad-capable formats
 * (N-Quads, TriG) each statement is emitted as a quad, preserving named graph context.
 *
 * <p>Lifecycle contract (matches other {@link RdfSerializer} implementations):
 * <ul>
 *   <li>{@link #start} on an already-active session throws {@link IllegalStateException}</li>
 *   <li>{@link #write} before {@link #start} or after {@link #end}/{@link #close} throws
 *       {@link IllegalStateException}</li>
 *   <li>{@link #close} is idempotent and does <strong>not</strong> close the caller's
 *       {@link OutputStream}</li>
 * </ul>
 *
 * <p>Jena-specific exceptions (e.g. {@link org.apache.jena.riot.RiotException}) are wrapped in
 * {@link RdfSerializationException} so callers of the SPI do not depend on Jena types.
 */
final class JenaStreamingSerializer implements RdfSerializer {

    private final Lang lang;

    private final boolean quads;

    private StreamRDF stream;

    private OutputStream output;

    JenaStreamingSerializer(Lang lang) {
        this.lang = Objects.requireNonNull(lang, "lang");
        this.quads = RDFLanguages.isQuads(lang);
    }

    @Override
    public void start(OutputStream output, Map<String, String> namespaces) {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(namespaces, "namespaces");
        if (stream != null) {
            throw new IllegalStateException("start() called while a session is already active");
        }
        this.output = output;
        try {
            // Force Turtle 1.1 directive style ("@prefix"/"@base") for backward compatibility
            // with tooling that diffs output against Turtle 1.1 baselines. Jena 5 defaults to
            // Turtle 1.2 / SPARQL-style PREFIX and BASE directives.
            var context = new Context();
            context.set(RIOT.symTurtleDirectiveStyle, "at");
            this.stream = StreamRDFWriter.getWriterStream(output, lang, context);
            stream.start();
            namespaces.forEach(stream::prefix);
        } catch (RuntimeException runtimeException) {
            this.stream = null;
            this.output = null;
            throw new RdfSerializationException(
                    "Failed to start Jena %s streaming session".formatted(lang.getName()), runtimeException);
        }
    }

    @Override
    public void write(Statement statement) {
        if (stream == null) {
            throw new IllegalStateException("write() called outside of an active serialization session");
        }
        try {
            var quad = JenaConverters.toQuad(statement);
            if (quads) {
                stream.quad(quad);
            } else {
                stream.triple(quad.asTriple());
            }
        } catch (RuntimeException runtimeException) {
            throw new RdfSerializationException(
                    "Failed to write statement to Jena %s streaming serializer".formatted(lang.getName()),
                    runtimeException);
        }
    }

    @Override
    public void flush() {
        if (output != null) {
            try {
                output.flush();
            } catch (IOException ioException) {
                throw new UncheckedIOException(ioException);
            }
        }
    }

    @Override
    public void end() {
        if (stream != null) {
            var currentOutput = output;
            try {
                stream.finish();
            } catch (RuntimeException runtimeException) {
                stream = null;
                output = null;
                throw new RdfSerializationException(
                        "Failed to end Jena %s streaming session".formatted(lang.getName()), runtimeException);
            }
            try {
                flushOutput(currentOutput);
            } finally {
                stream = null;
                output = null;
            }
        }
    }

    private void flushOutput(OutputStream outputStream) {
        try {
            outputStream.flush();
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }

    @Override
    public void close() {
        stream = null;
        output = null;
    }
}
