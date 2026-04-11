package io.carml.logicalsourceresolver.sourceresolver;

import java.nio.charset.Charset;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.eclipse.rdf4j.model.IRI;

@UtilityClass
public class Encodings {

    public static Optional<Charset> resolveCharset(IRI encoding) {
        return io.carml.util.Encodings.resolveCharset(encoding);
    }
}
