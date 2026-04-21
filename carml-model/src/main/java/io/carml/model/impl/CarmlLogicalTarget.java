package io.carml.model.impl;

import io.carml.model.LogicalTarget;
import io.carml.model.Resource;
import io.carml.model.Target;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlLogicalTarget extends CarmlResource implements LogicalTarget {

    private Target target;

    private IRI serialization;

    private IRI encoding;

    private IRI compression;

    @RdfProperty(Rml.target)
    @Override
    public Target getTarget() {
        return target;
    }

    @RdfProperty(Rml.serialization)
    @Override
    public IRI getSerialization() {
        return serialization;
    }

    @RdfProperty(Rml.encoding)
    @Override
    public IRI getEncoding() {
        return encoding;
    }

    @RdfProperty(Rml.compression)
    @Override
    public IRI getCompression() {
        return compression;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        if (target != null) {
            return Set.of(target);
        }

        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.LogicalTarget);

        if (serialization != null) {
            modelBuilder.add(Rdf.Rml.serialization, serialization);
        }
        if (encoding != null) {
            modelBuilder.add(Rdf.Rml.encoding, encoding);
        }
        if (compression != null) {
            modelBuilder.add(Rdf.Rml.compression, compression);
        }
    }
}
