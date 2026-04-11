package io.carml.logicalsourceresolver.sourceresolver;

import java.io.InputStream;
import java.io.UncheckedIOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.eclipse.rdf4j.model.IRI;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Compressions {

    public static InputStream decompress(InputStream inputStream, IRI compression) {
        try {
            return io.carml.util.Compressions.decompress(inputStream, compression);
        } catch (UncheckedIOException uncheckedIOException) {
            throw new SourceResolverException(uncheckedIOException.getMessage(), uncheckedIOException.getCause());
        }
    }
}
