package io.carml.engine.target;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

/**
 * {@link TargetWriter} implementation for file-based RML targets. Writes RDF statements to a file
 * with configurable serialization format, compression, and encoding.
 *
 * <p>The writer manages the lifecycle of the underlying output stream and RDF4J {@link RDFWriter}:
 *
 * <ul>
 *   <li>{@link #open()} creates parent directories, opens the file, applies compression and
 *       encoding, and starts the RDF document.
 *   <li>{@link #write(Statement)} delegates to the RDF writer.
 *   <li>{@link #flush()} flushes the underlying output stream.
 *   <li>{@link #close()} ends the RDF document and closes all streams.
 * </ul>
 */
@Slf4j
@Builder
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class FileTargetWriter implements TargetWriter {

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final String GZIP = RML_NS + "gzip";

    private static final String ZIP = RML_NS + "zip";

    private static final String NONE = RML_NS + "none";

    private final Path filePath;

    private final RDFFormat rdfFormat;

    private final IRI compression;

    private final Charset charset;

    private OutputStream outputStream;

    private RDFWriter rdfWriter;

    @Override
    public void open() {
        try {
            createParentDirectories();
            outputStream = new BufferedOutputStream(Files.newOutputStream(filePath));
            outputStream = applyCompression(outputStream);
            rdfWriter = createRdfWriter(outputStream);
            rdfWriter.startRDF();
            LOG.debug("Opened file target writer for {}", filePath);
        } catch (IOException ioException) {
            throw new UncheckedIOException("Failed to open file target %s".formatted(filePath), ioException);
        }
    }

    @Override
    public void write(Statement statement) {
        rdfWriter.handleStatement(statement);
    }

    @Override
    public void flush() {
        try {
            outputStream.flush();
        } catch (IOException ioException) {
            throw new UncheckedIOException("Failed to flush file target %s".formatted(filePath), ioException);
        }
    }

    @Override
    public void close() {
        try {
            if (rdfWriter != null) {
                rdfWriter.endRDF();
            }
        } finally {
            closeOutputStream();
        }
        LOG.debug("Closed file target writer for {}", filePath);
    }

    private void createParentDirectories() throws IOException {
        var parent = filePath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
    }

    private OutputStream applyCompression(OutputStream stream) throws IOException {
        if (compression == null) {
            return stream;
        }

        var compressionValue = compression.stringValue();
        return switch (compressionValue) {
            case GZIP -> new GZIPOutputStream(stream);
            case ZIP -> {
                var zipStream = new ZipArchiveOutputStream(stream);
                zipStream.putArchiveEntry(
                        new ZipArchiveEntry(filePath.getFileName().toString()));
                yield zipStream;
            }
            default -> stream;
        };
    }

    private RDFWriter createRdfWriter(OutputStream stream) {
        if (charset != null) {
            Writer writer = new OutputStreamWriter(stream, charset);
            return Rio.createWriter(rdfFormat, writer);
        }
        return Rio.createWriter(rdfFormat, stream);
    }

    private void closeOutputStream() {
        if (outputStream != null) {
            try {
                if (outputStream instanceof ZipArchiveOutputStream zipStream) {
                    zipStream.closeArchiveEntry();
                }
                outputStream.close();
            } catch (IOException ioException) {
                LOG.warn("Failed to close file target {}", filePath, ioException);
            }
        }
    }
}
