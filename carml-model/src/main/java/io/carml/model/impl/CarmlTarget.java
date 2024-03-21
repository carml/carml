package io.carml.model.impl;

import io.carml.model.Resource;
import io.carml.model.Target;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rml;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlTarget extends CarmlResource implements Target {

    private IRI serialization;

    private IRI encoding;

    private IRI compression;

    @RdfProperty(Rml.serialization)
    @Override
    public IRI getSerialization() {
        return null;
    }

    @RdfProperty(Rml.encoding)
    @Override
    public IRI getEncoding() {
        return null;
    }

    @RdfProperty(Rml.compression)
    @Override
    public IRI getCompression() {
        return null;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {}
}
