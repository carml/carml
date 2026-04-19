package io.carml.output;

import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;

/**
 * Buffering {@link RdfSerializer} backed by RDF4J Rio. Collects statements into an in-memory
 * {@link Model}, then writes the whole model via
 * {@link Rio#write(Iterable, OutputStream, RDFFormat)} on {@link #end()}. This enables
 * pretty-printing features such as prefix shortening, blank node inlining, and sorted output for
 * block-structured formats (Turtle, TriG, RDF/XML, JSON-LD).
 *
 * <p>Not suitable for very large datasets: the entire set of statements is held in memory until
 * {@link #end()} is called.
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
 */
final class RioModelSerializer implements RdfSerializer {

    private final RDFFormat format;

    private Model model;

    private OutputStream output;

    RioModelSerializer(RDFFormat format) {
        this.format = Objects.requireNonNull(format, "format");
    }

    @Override
    public void start(OutputStream output, Map<String, String> namespaces) {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(namespaces, "namespaces");
        if (model != null) {
            throw new IllegalStateException("start() called while a session is already active");
        }
        this.model = new LinkedHashModel();
        this.output = output;
        namespaces.forEach(model::setNamespace);
    }

    @Override
    public void write(Statement statement) {
        if (model == null) {
            throw new IllegalStateException("write() called outside of an active serialization session");
        }
        model.add(statement);
    }

    @Override
    public void flush() {
        // Pretty-mode buffers until end(); mid-stream flush is a no-op.
    }

    @Override
    public void end() {
        if (model != null) {
            try {
                Rio.write(model, output, format, prettyConfig());
            } catch (RDFHandlerException rdfHandlerException) {
                throw new RdfSerializationException(
                        "Failed to write buffered model to Rio %s serializer".formatted(format.getName()),
                        rdfHandlerException);
            } finally {
                model = null;
                output = null;
            }
        }
    }

    @Override
    public void close() {
        model = null;
        output = null;
    }

    /**
     * Returns a fresh pretty-print {@link WriterConfig} on each invocation. Enables blank node
     * inlining ({@code [] a [] .} syntax) in addition to Rio's default pretty-print behavior.
     * Matches the output of {@code carml-jar}'s pre-SPI {@code Rdf4jOutputHandler} so downstream
     * users diffing output files see no regression.
     *
     * <p>A new instance is returned on each call so that any downstream mutation of the config
     * cannot leak back into a shared static and corrupt subsequent serialization sessions
     * process-wide.
     */
    private static WriterConfig prettyConfig() {
        return new WriterConfig()
                .set(BasicWriterSettings.PRETTY_PRINT, true)
                .set(BasicWriterSettings.INLINE_BLANK_NODES, true);
    }
}
