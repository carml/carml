package io.carml.model.impl;

import io.carml.model.Source;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.util.Objects;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@ToString(callSuper = true)
@Setter
public abstract class CarmlSource extends CarmlResource implements Source {

    private IRI encoding;

    @Singular
    private Set<Object> nulls;

    private IRI compression;

    @RdfProperty(Rml.encoding)
    @Override
    public IRI getEncoding() {
        return encoding;
    }

    @RdfProperty(Rml.NULL)
    @Override
    public Set<Object> getNulls() {
        return nulls;
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
        nulls.forEach(nullPattern -> builder.add(Rdf.Rml.NULL, nullPattern));
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
                && Objects.equals(nulls, that.getNulls())
                && Objects.equals(compression, that.getCompression());
    }

    @Override
    public int hashCode() {
        return Objects.hash(encoding, nulls, compression);
    }
}
