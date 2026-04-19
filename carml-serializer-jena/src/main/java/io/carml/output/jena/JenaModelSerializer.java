package io.carml.output.jena;

import io.carml.output.RdfSerializationException;
import io.carml.output.RdfSerializer;
import io.carml.util.jena.JenaConverters;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFWriter;
import org.apache.jena.riot.RIOT;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.eclipse.rdf4j.model.Statement;

/**
 * Buffering {@link RdfSerializer} backed by Apache Jena. Collects statements into an in-memory
 * {@link DatasetGraph}, then writes the complete dataset (or its default graph for triples-only
 * formats) via {@link RDFWriter} on {@link #end()}. This enables pretty-printing features such as
 * prefix shortening, blank node inlining, and sorted output for block-structured formats (Turtle,
 * TriG, RDF/XML, JSON-LD).
 *
 * <p>Not suitable for very large datasets: the entire set of statements is held in memory until
 * {@link #end()} is called.
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
 * <p>Jena-specific exceptions are wrapped in {@link RdfSerializationException} so callers of the
 * SPI do not depend on Jena types.
 */
final class JenaModelSerializer implements RdfSerializer {

    private final Lang lang;

    private final boolean quads;

    private DatasetGraph datasetGraph;

    private OutputStream output;

    JenaModelSerializer(Lang lang) {
        this.lang = Objects.requireNonNull(lang, "lang");
        this.quads = RDFLanguages.isQuads(lang);
    }

    @Override
    public void start(OutputStream output, Map<String, String> namespaces) {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(namespaces, "namespaces");
        if (datasetGraph != null) {
            throw new IllegalStateException("start() called while a session is already active");
        }
        this.datasetGraph = DatasetGraphFactory.create();
        this.output = output;
        try {
            var prefixMapping = datasetGraph.getDefaultGraph().getPrefixMapping();
            namespaces.forEach(prefixMapping::setNsPrefix);
        } catch (RuntimeException runtimeException) {
            this.datasetGraph = null;
            this.output = null;
            throw new RdfSerializationException(
                    "Failed to start Jena %s pretty session".formatted(lang.getName()), runtimeException);
        }
    }

    @Override
    public void write(Statement statement) {
        if (datasetGraph == null) {
            throw new IllegalStateException("write() called outside of an active serialization session");
        }
        datasetGraph.add(JenaConverters.toQuad(statement));
    }

    @Override
    public void flush() {
        // Pretty-mode buffers until end(); mid-stream flush is a no-op.
    }

    @Override
    public void end() {
        if (datasetGraph != null) {
            try {
                // Force Turtle 1.1 directive style ("@prefix"/"@base") for backward compatibility
                // with tooling that diffs output against Turtle 1.1 baselines. Jena 5 defaults to
                // Turtle 1.2 / SPARQL-style PREFIX and BASE directives.
                if (quads) {
                    RDFWriter.source(datasetGraph)
                            .lang(lang)
                            .set(RIOT.symTurtleDirectiveStyle, "at")
                            .output(output);
                } else {
                    RDFWriter.source(datasetGraph.getDefaultGraph())
                            .lang(lang)
                            .set(RIOT.symTurtleDirectiveStyle, "at")
                            .output(output);
                }
            } catch (RuntimeException runtimeException) {
                throw new RdfSerializationException(
                        "Failed to write buffered model to Jena %s serializer".formatted(lang.getName()),
                        runtimeException);
            } finally {
                datasetGraph = null;
                output = null;
            }
        }
    }

    @Override
    public void close() {
        datasetGraph = null;
        output = null;
    }
}
