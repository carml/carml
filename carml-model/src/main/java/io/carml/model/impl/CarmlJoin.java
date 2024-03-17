package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.ChildMap;
import io.carml.model.Join;
import io.carml.model.ParentMap;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
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
public class CarmlJoin extends CarmlResource implements Join {

    private ChildMap childMap;

    private ParentMap parentMap;

    // TODO backwards comp?
    @RdfProperty(Rml.childMap)
    @RdfType(CarmlChildMap.class)
    @Override
    public ChildMap getChildMap() {
        return childMap;
    }

    // TODO backwards comp?
    @RdfProperty(Rml.parentMap)
    @RdfType(CarmlParentMap.class)
    @Override
    public ParentMap getParentMap() {
        return parentMap;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();

        if (childMap != null) {
            builder.add(childMap);
        }
        if (parentMap != null) {
            builder.add(parentMap);
        }

        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.Join);

        if (childMap != null) {
            modelBuilder.add(Rml.childMap, childMap.getAsResource());
        }
        if (parentMap != null) {
            modelBuilder.add(Rml.parentMap, parentMap.getAsResource());
        }
    }
}
