package io.carml.engine.target;

import io.carml.output.RdfSerializer;
import io.carml.output.RdfSerializerFactory;
import io.carml.output.SerializerMode;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.WriterOutputStream;
import org.eclipse.rdf4j.model.Statement;

/**
 * {@link TargetWriter} implementation that writes RDF statements to a caller-provided
 * {@link OutputStream}. Unlike {@link FileTargetWriter}, this writer does <strong>not</strong>
 * own the underlying stream: {@link #close()} ends the serializer and releases writer state but
 * leaves the caller-provided {@link OutputStream} open for the caller to close.
 *
 * <p>Use this writer to plug an externally-managed sink (stdout, an already-open file, a
 * socket) into a {@link TargetRouter} as the default writer for statements that do not have a
 * declared {@code rml:logicalTarget}. A charset wrapper is applied when a non-UTF-8 charset is
 * configured, matching {@link FileTargetWriter}'s transcoding behaviour. Compression is NOT
 * applied — the caller owns any stream wrapping policy because they own the stream.
 *
 * <p><strong>Thread-safety:</strong> thread-safe. Concurrent {@link #write}, {@link #flush} and
 * {@link #close} calls are serialized via {@code synchronized}. This is required because the
 * {@link TargetRouter} may dispatch from multiple reactive threads when the engine evaluates
 * multiple TriplesMaps in parallel.
 *
 * <p><strong>Lifecycle contract:</strong>
 * <ul>
 *   <li>{@link #open()} creates the serializer via {@link RdfSerializerFactory} and calls
 *       {@link RdfSerializer#start}. Throws {@link IllegalStateException} if already open.
 *   <li>{@link #write(Statement)} / {@link #flush()} throw {@link IllegalStateException} when
 *       called outside the open-and-not-closed window.
 *   <li>{@link #close()} calls {@link RdfSerializer#end} and {@link RdfSerializer#close} but
 *       leaves the external {@link OutputStream} untouched. Idempotent.
 * </ul>
 */
@Slf4j
public class StreamTargetWriter implements TargetWriter {

    /**
     * Default {@link RdfSerializerFactory}. Single {@link java.util.ServiceLoader} scan at class
     * load — shared across all instances that do not configure one explicitly.
     */
    private static final RdfSerializerFactory DEFAULT_SERIALIZER_FACTORY = RdfSerializerFactory.create();

    private final OutputStream outputStream;

    private final String format;

    private final SerializerMode mode;

    private final RdfSerializerFactory serializerFactory;

    private final Map<String, String> namespaces;

    private final Charset charset;

    private OutputStream activeStream;

    private RdfSerializer serializer;

    /**
     * Package-private constructor used by the Lombok-generated builder.
     */
    @Builder
    StreamTargetWriter(
            OutputStream outputStream,
            String format,
            SerializerMode mode,
            RdfSerializerFactory serializerFactory,
            Map<String, String> namespaces,
            Charset charset) {
        this.outputStream = outputStream;
        this.format = format;
        this.mode = mode != null ? mode : SerializerMode.STREAMING;
        this.serializerFactory = serializerFactory != null ? serializerFactory : DEFAULT_SERIALIZER_FACTORY;
        this.namespaces = namespaces != null ? namespaces : Map.of();
        this.charset = charset;
    }

    @Override
    public void open() {
        if (serializer != null) {
            throw new IllegalStateException("StreamTargetWriter already open");
        }
        try {
            activeStream = applyCharset(outputStream);
            serializer = serializerFactory.createSerializer(format, mode);
            serializer.start(activeStream, namespaces);
            LOG.debug("Opened stream target writer (format={}, mode={})", format, mode);
        } catch (RuntimeException | IOException failure) {
            // Partial-open cleanup: release serializer (if any) and the charset wrapper (if any,
            // and distinct from the caller's stream). The caller-owned outputStream is NEVER
            // closed — caller ownership is the defining invariant of this writer.
            cleanupOnFailedOpen();
            if (failure instanceof IOException ioException) {
                throw new UncheckedIOException(
                        "Failed to open stream target writer (format=%s)".formatted(format), ioException);
            }
            throw (RuntimeException) failure;
        }
    }

    @Override
    public synchronized void write(Statement statement) {
        if (serializer == null) {
            throw new IllegalStateException("StreamTargetWriter not open or already closed");
        }
        serializer.write(statement);
    }

    @Override
    public synchronized void flush() {
        if (serializer == null) {
            throw new IllegalStateException("StreamTargetWriter not open or already closed");
        }
        try {
            serializer.flush();
            if (activeStream != null && activeStream != outputStream) {
                activeStream.flush();
            }
            outputStream.flush();
        } catch (IOException ioException) {
            throw new UncheckedIOException("Failed to flush stream target writer", ioException);
        }
    }

    @Override
    public synchronized void close() {
        try {
            endSerializerThenReleaseWrapper();
        } finally {
            serializer = null;
            activeStream = null;
            LOG.debug("Closed stream target writer (format={}, mode={})", format, mode);
        }
    }

    /**
     * Ends the serializer, then releases any charset wrapper stream we own (not the
     * caller-provided {@link #outputStream}). Extracted to keep the close path within the
     * project's {@code NestedTryDepth=1} limit while preserving the invariant that the wrapper
     * is released even if {@link RdfSerializer#end()} throws.
     */
    private void endSerializerThenReleaseWrapper() {
        try {
            if (serializer != null) {
                serializer.end();
            }
        } finally {
            closeSerializerQuietly();
            closeCharsetWrapperQuietly();
        }
    }

    private void closeSerializerQuietly() {
        if (serializer == null) {
            return;
        }
        try {
            serializer.close();
        } catch (RuntimeException runtimeException) {
            LOG.warn("Failed to close serializer for stream target writer (format={})", format, runtimeException);
        }
    }

    /**
     * Closes the charset transcoding wrapper, if one was installed. Does not close the
     * caller-owned {@link #outputStream} — caller ownership is the defining invariant.
     */
    private void closeCharsetWrapperQuietly() {
        if (activeStream == null || activeStream == outputStream) {
            return;
        }
        try {
            activeStream.close();
        } catch (IOException ioException) {
            LOG.warn("Failed to close charset wrapper for stream target writer (format={})", format, ioException);
        }
    }

    private void cleanupOnFailedOpen() {
        closeSerializerQuietly();
        closeCharsetWrapperQuietly();
        serializer = null;
        activeStream = null;
    }

    /**
     * Applies a charset transcoding wrapper when a non-UTF-8 charset is configured. Mirrors
     * {@link FileTargetWriter#applyCharset}: serializers always emit UTF-8, so we decode to
     * characters and re-encode to the target charset. End-to-end coverage is provided by the
     * charset transcoding tests in {@code StreamTargetWriterTest}.
     */
    private OutputStream applyCharset(OutputStream stream) throws IOException {
        if (charset == null || charset.equals(StandardCharsets.UTF_8)) {
            return stream;
        }
        var writer = new OutputStreamWriter(stream, charset);
        return WriterOutputStream.builder()
                .setWriter(writer)
                .setCharset(StandardCharsets.UTF_8)
                .get();
    }
}
