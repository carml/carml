package io.carml.output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

/**
 * Streaming {@link RdfSerializer} backed by an RDF4J Rio {@link RDFWriter}.
 *
 * <p>Writes statements one-by-one without buffering the full model. Suitable for large datasets.
 * Output quality depends on the Rio writer: line-based formats (N-Triples, N-Quads) produce
 * streamable output, while block-structured formats (Turtle, TriG, RDF/XML) may emit more verbose
 * output than a {@link SerializerMode#PRETTY}-mode serializer.
 *
 * <p>Lifecycle contract (matches {@link AbstractFastRdfSerializer}):
 * <ul>
 *   <li>{@link #start} on an already-active session throws {@link IllegalStateException}</li>
 *   <li>{@link #write} before {@link #start} or after {@link #end}/{@link #close} throws
 *       {@link IllegalStateException}</li>
 *   <li>{@link #close} is idempotent and does <strong>not</strong> close the caller's
 *       {@link OutputStream}</li>
 * </ul>
 *
 * <p>Exceptions thrown by Rio ({@link RDFHandlerException}) are wrapped in
 * {@link RdfSerializationException} so callers of the SPI do not depend on Rio types.
 *
 * <p><strong>flush() caveat:</strong> for line-based formats (N-Triples, N-Quads) {@link #flush()}
 * reliably flushes the underlying output stream. For block-structured formats (Turtle, TriG,
 * RDF/XML, JSON-LD, N3), Rio writers buffer in an internal {@code java.io.Writer} that the SPI
 * cannot access; {@link #flush()} is therefore <strong>best-effort</strong> and may leave partial
 * output in Rio's internal buffer until {@link #end()} flushes everything. Checkpoint-driven
 * streaming pipelines that require strict flush semantics should prefer line-based formats or the
 * Fast serializer providers.
 */
final class RioStreamingSerializer implements RdfSerializer {

    private final RDFFormat format;

    private RDFWriter writer;

    private OutputStream output;

    RioStreamingSerializer(RDFFormat format) {
        this.format = Objects.requireNonNull(format, "format");
    }

    @Override
    public void start(OutputStream output, Map<String, String> namespaces) {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(namespaces, "namespaces");
        if (writer != null) {
            throw new IllegalStateException("start() called while a session is already active");
        }
        this.output = output;
        try {
            this.writer = Rio.createWriter(format, output);
            writer.startRDF();
            namespaces.forEach(writer::handleNamespace);
        } catch (RDFHandlerException rdfHandlerException) {
            this.writer = null;
            this.output = null;
            throw new RdfSerializationException(
                    "Failed to start Rio %s serialization session".formatted(format.getName()), rdfHandlerException);
        }
    }

    @Override
    public void write(Statement statement) {
        if (writer == null) {
            throw new IllegalStateException("write() called outside of an active serialization session");
        }
        try {
            writer.handleStatement(statement);
        } catch (RDFHandlerException rdfHandlerException) {
            throw new RdfSerializationException(
                    "Failed to write statement to Rio %s serializer".formatted(format.getName()), rdfHandlerException);
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
        if (writer != null) {
            try {
                writer.endRDF();
            } catch (RDFHandlerException rdfHandlerException) {
                writer = null;
                output = null;
                throw new RdfSerializationException(
                        "Failed to end Rio %s serialization session".formatted(format.getName()), rdfHandlerException);
            }
            try {
                flush();
            } finally {
                writer = null;
                output = null;
            }
        }
    }

    @Override
    public void close() {
        writer = null;
        output = null;
    }
}
