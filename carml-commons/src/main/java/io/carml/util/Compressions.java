package io.carml.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.zip.GZIPInputStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.eclipse.rdf4j.model.IRI;

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
}
