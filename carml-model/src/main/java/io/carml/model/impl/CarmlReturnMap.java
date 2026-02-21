package io.carml.model.impl;

import io.carml.model.Resource;
import io.carml.model.ReturnMap;
import io.carml.vocab.Rdf;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlReturnMap extends CarmlExpressionMap implements ReturnMap {

    @Override
    public Set<Resource> getReferencedResources() {
        return getReferencedResourcesBase();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.ReturnMap);
        addTriplesBase(modelBuilder);
    }
}
