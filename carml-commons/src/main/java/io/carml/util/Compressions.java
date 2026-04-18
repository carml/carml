package io.carml.util;

import java.io.BufferedInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.eclipse.rdf4j.model.IRI;
import org.jspecify.annotations.NonNull;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Compressions {

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final String GZIP = RML_NS + "gzip";

    private static final String ZIP = RML_NS + "zip";

    private static final String TARXZ = RML_NS + "tarxz";

    private static final String TARGZ = RML_NS + "targz";

    private static final String NONE = RML_NS + "none";

    public static InputStream decompress(InputStream inputStream, IRI compression) {
        if (compression == null) {
            return inputStream;
        }
        var compressionValue = compression.stringValue();

        switch (compressionValue) {
            case GZIP:
                try {
                    return new GZIPInputStream(new BufferedInputStream(inputStream));
                } catch (IOException ioException) {
                    throw new UncheckedIOException("Could not open gzip input", ioException);
                }
            case ZIP:
                try {
                    var zip = new ZipArchiveInputStream(new BufferedInputStream(inputStream));
                    zip.getNextEntry();
                    return zip;
                } catch (IOException ioException) {
                    throw new UncheckedIOException("Could not open zip input", ioException);
                }
            case TARXZ:
                try {
                    var tar = new TarArchiveInputStream(
                            new XZCompressorInputStream(new BufferedInputStream(inputStream)));
                    tar.getNextEntry();
                    return tar;
                } catch (IOException ioException) {
                    throw new UncheckedIOException("Could not open tar+xz input", ioException);
                }
            case TARGZ:
                try {
                    var tar = new TarArchiveInputStream(new GZIPInputStream(new BufferedInputStream(inputStream)));
                    tar.getNextEntry();
                    return tar;
                } catch (IOException ioException) {
                    throw new UncheckedIOException("Could not open tar+gz input", ioException);
                }
            case NONE:
            default:
                return inputStream;
        }
    }

    /**
     * Wraps the given {@code outputStream} in a compression stream according to the RML compression
     * IRI. The returned stream compresses bytes written to it into {@code outputStream}.
     *
     * <p>Supported compressions:
     * <ul>
     *   <li>{@code rml:gzip} &rarr; {@link GZIPOutputStream}</li>
     *   <li>{@code rml:zip} &rarr; {@link ZipArchiveOutputStream} containing a single entry named
     *       {@code entryName}. The returned stream closes the zip entry on {@link OutputStream#close()
     *       close()} before delegating, so callers may safely use {@code try-with-resources}.</li>
     *   <li>{@code null} and {@code rml:none} &rarr; returns {@code outputStream} unchanged.</li>
     * </ul>
     *
     * <p>Compressions that cannot sensibly be produced in a single-stream write path ({@code
     * rml:tarxz}, {@code rml:targz}) throw {@link UnsupportedOperationException}. Decompression for
     * these formats is still supported by {@link #decompress(InputStream, IRI)}.
     *
     * @param outputStream the underlying output stream to write compressed bytes to
     * @param compression the RML compression IRI, or {@code null} for no compression
     * @param entryName the entry name for archive formats (ZIP); ignored for stream-only formats
     * @return an output stream that compresses bytes into {@code outputStream}
     * @throws UncheckedIOException if the underlying compression stream cannot be opened
     * @throws UnsupportedOperationException if the compression is not supported for writing
     */
    public static OutputStream compress(OutputStream outputStream, IRI compression, String entryName) {
        if (compression == null) {
            return outputStream;
        }
        var compressionValue = compression.stringValue();

        switch (compressionValue) {
            case GZIP:
                try {
                    return new GZIPOutputStream(outputStream);
                } catch (IOException ioException) {
                    throw new UncheckedIOException("Could not open gzip output", ioException);
                }
            case ZIP:
                try {
                    var zip = new ZipArchiveOutputStream(outputStream);
                    zip.putArchiveEntry(new ZipArchiveEntry(entryName));
                    return new ZipEntryClosingOutputStream(zip);
                } catch (IOException ioException) {
                    throw new UncheckedIOException("Could not open zip output", ioException);
                }
            case TARXZ, TARGZ:
                throw new UnsupportedOperationException(
                        "compression not supported for write: %s".formatted(compressionValue));
            case NONE:
            default:
                return outputStream;
        }
    }

    /**
     * Wraps a {@link ZipArchiveOutputStream} so that {@link #close()} first closes the current
     * archive entry, then delegates to the underlying zip stream. This matches the {@code
     * try-with-resources} contract callers expect for the stream returned by {@link
     * #compress(OutputStream, IRI, String)}.
     *
     * <p><strong>Thread safety:</strong> not thread-safe. Like the underlying
     * {@link ZipArchiveOutputStream}, callers must externally synchronize concurrent access.
     */
    private static final class ZipEntryClosingOutputStream extends FilterOutputStream {

        private final ZipArchiveOutputStream zip;

        private boolean closed;

        private ZipEntryClosingOutputStream(ZipArchiveOutputStream zip) {
            super(zip);
            this.zip = zip;
        }

        @Override
        public void write(int byteValue) throws IOException {
            // Bypass FilterOutputStream's identical-looking default to avoid double indirection.
            zip.write(byteValue);
        }

        @Override
        public void write(byte @NonNull [] bytes, int off, int len) throws IOException {
            // FilterOutputStream.write(byte[],int,int) calls write(int) per-byte, which is slow and
            // wrong for zip output. Delegate directly.
            zip.write(bytes, off, len);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            zip.closeArchiveEntry();
            zip.close();
        }
    }
}
