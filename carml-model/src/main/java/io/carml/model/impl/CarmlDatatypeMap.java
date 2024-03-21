package io.carml.model.impl;

import io.carml.model.DatatypeMap;
import io.carml.model.Resource;
import io.carml.model.TermType;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CarmlDatatypeMap extends CarmlTermMap implements DatatypeMap {

    @Override
    public Set<Resource> getReferencedResources() {
        return getReferencedResourcesBase();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rml.DatatypeMap);

        addTriplesBase(modelBuilder);
    }

    @RdfProperty(Rml.termType)
    @RdfProperty(Rr.termType)
    @Override
    public TermType getTermType() {
        if (super.getTermType() != null) {
            return super.getTermType();
        }

        return TermType.IRI;
    }
}
