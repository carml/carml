package io.carml.engine.target;

import io.carml.output.RdfSerializer;
import io.carml.output.RdfSerializerFactory;
import io.carml.output.SerializerMode;
import io.carml.util.Compressions;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.WriterOutputStream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;

/**
 * {@link TargetWriter} implementation for file-based RML targets. Writes RDF statements to a file
 * via the {@link RdfSerializer} SPI with configurable serialization format, compression, and
 * encoding.
 *
 * <p>The writer manages the lifecycle of the underlying output stream and the {@link RdfSerializer}:
 *
 * <ul>
 *   <li>{@link #open()} creates parent directories, opens the file, applies compression and
 *       encoding wrappers, selects an {@link RdfSerializer} for {@link #format} via
 *       {@link RdfSerializerFactory}, and starts the RDF document.
 *   <li>{@link #write(Statement)} delegates to the serializer.
 *   <li>{@link #flush()} flushes the serializer and the underlying output stream. Calling
 *       {@code flush} before {@link #open()} or after {@link #close()} throws
 *       {@link IllegalStateException} for symmetry with {@link #write(Statement)}.
 *   <li>{@link #close()} ends the serializer and closes the underlying stream chain. The
 *       serializer's {@code close()} contract does <strong>not</strong> close the caller's output
 *       stream, so this class owns stream closure.
 * </ul>
 *
 * <p>The public {@link #builder()} surface exposes only configuration fields (file path, format,
 * mode, serializer factory, namespaces, compression, charset). The internal lifecycle fields
 * ({@code outputStream}, {@code serializer}) are deliberately <em>not</em> builder-settable.
 *
 * <p><strong>Lifecycle contract:</strong> {@link #open()} must be called exactly once before any
 * {@link #write(Statement)} / {@link #flush()} calls, and at most once per writer instance.
 * Calling {@code open()} a second time on a writer that is already open throws
 * {@link IllegalStateException}. To reuse configuration for multiple files, build a fresh writer
 * via the {@link #builder()}.
 *
 * <p><strong>Thread-safety:</strong> <em>not</em> thread-safe. When used with
 * {@link TargetRouter} in a parallel pipeline (multiple reactive threads calling
 * {@link #write(Statement)} concurrently), external synchronization is required — consider
 * {@link StreamTargetWriter} for concurrent scenarios (it serializes writes internally), or wrap
 * this writer in a synchronized adapter. See {@link TargetRouter} Javadoc for the router's
 * delegation contract.
 */
@Slf4j
public class FileTargetWriter implements TargetWriter {

    /**
     * Default {@link RdfSerializerFactory}. Initialized once at class load to avoid a
     * {@link java.util.ServiceLoader} scan on every {@code builder().build()} call.
     */
    private static final RdfSerializerFactory DEFAULT_SERIALIZER_FACTORY = RdfSerializerFactory.create();

    private final Path filePath;

    /**
     * Bare RDF format token understood by the {@link RdfSerializer} SPI (e.g. {@code "nt"},
     * {@code "ttl"}, {@code "nq"}).
     */
    private final String format;

    /**
     * Serialization mode. Defaults to {@link SerializerMode#STREAMING}.
     */
    private final SerializerMode mode;

    /**
     * Factory used to create the underlying {@link RdfSerializer}. Defaults to a
     * {@link RdfSerializerFactory} instance scanned once via {@link java.util.ServiceLoader} at
     * class load time (see {@link #DEFAULT_SERIALIZER_FACTORY}).
     */
    private final RdfSerializerFactory serializerFactory;

    /**
     * Namespace prefix-to-IRI mappings passed to {@link RdfSerializer#start(OutputStream, Map)}.
     * Defaults to an empty map.
     */
    private final Map<String, String> namespaces;

    private final IRI compression;

    private final Charset charset;

    private OutputStream outputStream;

    private RdfSerializer serializer;

    /**
     * Package-private constructor used by the Lombok-generated builder. Only configuration fields
     * are accepted; lifecycle fields ({@code outputStream}, {@code serializer}) are deliberately
     * omitted so they cannot be set by callers.
     */
    @Builder
    FileTargetWriter(
            Path filePath,
            String format,
            SerializerMode mode,
            RdfSerializerFactory serializerFactory,
            Map<String, String> namespaces,
            IRI compression,
            Charset charset) {
        this.filePath = filePath;
        this.format = format;
        this.mode = mode != null ? mode : SerializerMode.STREAMING;
        this.serializerFactory = serializerFactory != null ? serializerFactory : DEFAULT_SERIALIZER_FACTORY;
        this.namespaces = namespaces != null ? namespaces : Map.of();
        this.compression = compression;
        this.charset = charset;
    }

    /**
     * Opens the underlying file and initializes the serializer. Must be called exactly once before
     * any {@link #write(Statement)} or {@link #flush()} calls.
     *
     * @throws IllegalStateException if this writer has already been opened
     * @throws UncheckedIOException if the file cannot be created or opened
     */
    @Override
    public void open() {
        if (serializer != null) {
            throw new IllegalStateException("FileTargetWriter already open: %s".formatted(filePath));
        }
        try {
            createParentDirectories();
            var raw = new BufferedOutputStream(Files.newOutputStream(filePath));
            initializeSerializerChain(raw);
            LOG.debug("Opened file target writer for {}", filePath);
        } catch (IOException ioException) {
            throw new UncheckedIOException("Failed to open file target %s".formatted(filePath), ioException);
        }
    }

    /**
     * Applies compression + charset transcoding to the raw file stream and starts the serializer.
     * On any failure, closes whatever portion of the stream chain was successfully opened and
     * rethrows so {@link #open()} can decide whether to wrap the exception.
     */
    private void initializeSerializerChain(OutputStream raw) throws IOException {
        OutputStream compressed = null;
        try {
            compressed = Compressions.compress(
                    raw, compression, filePath.getFileName().toString());
            outputStream = applyCharset(compressed);
            serializer = serializerFactory.createSerializer(format, mode);
            serializer.start(outputStream, namespaces);
        } catch (RuntimeException | IOException failure) {
            closePartiallyOpened(raw, compressed);
            throw failure;
        }
    }

    @Override
    public void write(Statement statement) {
        if (serializer == null) {
            throw new IllegalStateException("FileTargetWriter not open or already closed: %s".formatted(filePath));
        }
        serializer.write(statement);
    }

    @Override
    public void flush() {
        if (serializer == null) {
            throw new IllegalStateException("FileTargetWriter not open or already closed: %s".formatted(filePath));
        }
        try {
            serializer.flush();
            if (outputStream != null) {
                outputStream.flush();
            }
        } catch (IOException ioException) {
            throw new UncheckedIOException("Failed to flush file target %s".formatted(filePath), ioException);
        }
    }

    @Override
    public void close() {
        try {
            endSerializerThenCloseChain();
        } finally {
            serializer = null;
            outputStream = null;
            LOG.debug("Closed file target writer for {}", filePath);
        }
    }

    /**
     * Ends the serializer and unconditionally closes the serializer and the underlying output
     * stream chain, in that order. Extracted to satisfy the project's nested-try-depth rule while
     * preserving the guarantee that every resource is released even if an earlier step throws.
     */
    private void endSerializerThenCloseChain() {
        try {
            if (serializer != null) {
                serializer.end();
            }
        } finally {
            closeSerializerQuietly();
            closeOutputStream();
        }
    }

    /**
     * Closes the serializer, swallowing any {@link RuntimeException} it throws and logging a
     * warning. Used from {@link #close()} and {@link #closePartiallyOpened} where the caller has
     * either already completed the primary close sequence (so propagating a secondary failure
     * would mask it) or is already handling a higher-priority failure (open-time init failure).
     */
    private void closeSerializerQuietly() {
        if (serializer == null) {
            return;
        }
        try {
            serializer.close();
        } catch (RuntimeException runtimeException) {
            LOG.warn("Failed to close serializer for file target {}", filePath, runtimeException);
        }
    }

    private void createParentDirectories() throws IOException {
        var parent = filePath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
    }

    /**
     * Applies a charset transcoding wrapper around the given stream when a non-UTF-8 charset is
     * configured. Serializers always emit UTF-8 bytes; this chain decodes them to characters and
     * re-encodes to the target charset. For UTF-8 or {@code null} charset, no wrapping is needed.
     *
     * <p>Package-private for test access only — tests may override this method via a subclass to
     * inject a close-failing wrapper and exercise the close-time error-propagation contract in
     * {@link #endSerializerThenCloseChain()}. Production code must not depend on this visibility.
     */
    OutputStream applyCharset(OutputStream compressed) throws IOException {
        if (charset == null || charset.equals(StandardCharsets.UTF_8)) {
            return compressed;
        }
        var writer = new OutputStreamWriter(compressed, charset);
        return WriterOutputStream.builder()
                .setWriter(writer)
                .setCharset(StandardCharsets.UTF_8)
                .get();
    }

    private void closeOutputStream() {
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException ioException) {
                LOG.warn("Failed to close file target {}", filePath, ioException);
            }
        }
    }

    /**
     * Closes any partially-initialized streams on a failed {@link #open()}. Called when stream
     * wrapping or serializer initialization throws after the raw file stream was opened. The
     * serializer itself may or may not have been created; if it was, its {@link RdfSerializer#close()}
     * is invoked in a best-effort manner. Fields are reset so the writer can be garbage-collected
     * cleanly without leaking file descriptors.
     *
     * <p>Native resource safety: the {@code compressed} parameter is tracked separately so that
     * if compression wrapping succeeded but a later step (e.g. charset wrapping or serializer
     * init) threw, the {@link java.util.zip.Deflater}-owning stream is released rather than being
     * silently abandoned. The widest successfully opened stream is closed in priority order:
     * {@code outputStream} (wraps {@code compressed} or {@code raw}) &rarr; {@code compressed}
     * (wraps {@code raw}) &rarr; {@code raw}.
     */
    private void closePartiallyOpened(OutputStream raw, OutputStream compressed) {
        closeSerializerQuietly();
        serializer = null;
        OutputStream chainToClose;
        if (outputStream != null) {
            chainToClose = outputStream;
        } else if (compressed != null) {
            chainToClose = compressed;
        } else {
            chainToClose = raw;
        }
        try {
            chainToClose.close();
        } catch (IOException ioException) {
            LOG.warn("Failed to close partially-opened file target {}", filePath, ioException);
        }
        outputStream = null;
    }
}
