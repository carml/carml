package io.carml.logicalsourceresolver.sourceresolver;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.carml.vocab.Rml;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.eclipse.rdf4j.model.IRI;

@UtilityClass
public class Encodings {

    public static Optional<Charset> resolveCharset(IRI encoding) {
        if (encoding == null) {
            return Optional.empty();
        }

        if (encoding.stringValue().equals(Rml.UTF_8)) {
            return Optional.of(UTF_8);
        } else if (encoding.stringValue().equals(Rml.UTF_16)) {
            return Optional.of(StandardCharsets.UTF_16);
        } else {
            return Optional.empty();
        }
    }
}
