package io.carml.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.eclipse.rdf4j.model.IRI;

@UtilityClass
public class Encodings {

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final String RML_UTF_8 = RML_NS + "UTF-8";

    private static final String RML_UTF_16 = RML_NS + "UTF-16";

    public static Optional<Charset> resolveCharset(IRI encoding) {
        if (encoding == null) {
            return Optional.empty();
        }

        if (encoding.stringValue().equals(RML_UTF_8)) {
            return Optional.of(UTF_8);
        } else if (encoding.stringValue().equals(RML_UTF_16)) {
            return Optional.of(StandardCharsets.UTF_16);
        } else {
            return Optional.empty();
        }
    }
}
