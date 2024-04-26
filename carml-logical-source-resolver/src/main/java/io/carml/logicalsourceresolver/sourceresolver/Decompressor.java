package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.vocab.Rml;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiFunction;
import java.util.zip.GZIPInputStream;
import lombok.NoArgsConstructor;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.eclipse.rdf4j.model.IRI;

@NoArgsConstructor(staticName = "getInstance")
public class Decompressor implements BiFunction<InputStream, IRI, InputStream> {

    @Override
    public InputStream apply(InputStream inputStream, IRI compression) {
        if (compression == null) {
            return inputStream;
        }
        var compressionValue = compression.stringValue();

        switch (compressionValue) {
            case Rml.gzip:
                try {
                    return new GZIPInputStream(new BufferedInputStream(inputStream));
                } catch (IOException ioException) {
                    throw new SourceResolverException("Could not open gzip input", ioException);
                }
            case Rml.zip:
                try {
                    var zip = new ZipArchiveInputStream(new BufferedInputStream(inputStream));
                    zip.getNextEntry();
                    return zip;
                } catch (IOException ioException) {
                    throw new SourceResolverException("Could not open zip input", ioException);
                }
            case Rml.tarxz:
                try {
                    var tar = new TarArchiveInputStream(
                            new XZCompressorInputStream(new BufferedInputStream(inputStream)));
                    tar.getNextEntry();
                    return tar;
                } catch (IOException ioException) {
                    throw new SourceResolverException("Could not open tar+xz input", ioException);
                }
            case Rml.targz:
                try {
                    var tar = new TarArchiveInputStream(new GZIPInputStream(new BufferedInputStream(inputStream)));
                    tar.getNextEntry();
                    return tar;
                } catch (IOException ioException) {
                    throw new SourceResolverException("Could not open tar+gz input", ioException);
                }
            case Rml.none:
            default:
                return inputStream;
        }
    }
}
