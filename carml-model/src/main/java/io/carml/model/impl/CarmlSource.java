package io.carml.model.impl;

import io.carml.model.Source;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.util.Objects;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@ToString(callSuper = true)
public abstract class CarmlSource extends CarmlResource implements Source {

    @Setter
    private IRI encoding;

    private String nullPattern;

    @Setter
    private IRI compression;

    @RdfProperty(Rml.encoding)
    @Override
    public IRI getEncoding() {
        return encoding;
    }

    @RdfProperty(Rml.NULL)
    @Override
    public String getNull() {
        return nullPattern;
    }

    public void setNull(String nullPattern) {
        this.nullPattern = nullPattern;
    }

    @RdfProperty(Rml.compression)
    @Override
    public IRI getCompression() {
        return compression;
    }

    void addTriplesBase(ModelBuilder builder) {
        if (encoding != null) {
            builder.add(Rdf.Rml.encoding, encoding);
        }
        if (nullPattern != null) {
            builder.add(Rdf.Rml.nullPattern, nullPattern);
        }
        if (compression != null) {
            builder.add(Rdf.Rml.compression, compression);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CarmlSource that)) {
            return false;
        }

        return equalsSource(that);
    }

    protected boolean equalsSource(Source that) {
        return Objects.equals(encoding, that.getEncoding())
                && Objects.equals(nullPattern, that.getNull())
                && Objects.equals(compression, that.getCompression());
    }

    @Override
    public int hashCode() {
        return Objects.hash(encoding, nullPattern, compression);
    }
}
