package io.carml.model.impl;

import io.carml.model.LogicalTarget;
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
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlLogicalTarget extends CarmlResource implements LogicalTarget {

    private Target target;

    @RdfProperty(Rml.target)
    @Override
    public Target getTarget() {
        return target;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {}
}
