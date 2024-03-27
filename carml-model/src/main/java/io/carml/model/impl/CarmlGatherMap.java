package io.carml.model.impl;

import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.model.GatherMap;
import io.carml.model.ObjectMap;
import io.carml.model.Resource;
import io.carml.model.SubjectMap;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
abstract class CarmlGatherMap extends CarmlTermMap implements GatherMap {

    private IRI strategy;

    private IRI gatherAs;

    @Default
    private List<ObjectMap> gathers = List.of();

    private boolean allowEmptyListAndContainer;

    @RdfProperty(Rml.strategy)
    @Override
    public IRI getStrategy() {
        if (strategy == null) {
            return Rdf.Rml.append;
        }
        return strategy;
    }

    @RdfProperty(Rml.gatherAs)
    @Override
    public IRI getGatherAs() {
        return gatherAs;
    }

    @RdfProperty(Rml.gather)
    @RdfType(CarmlObjectMap.class)
    @Override
    public List<ObjectMap> getGathers() {
        return gathers;
    }

    @RdfProperty(Rml.allowEmptyListAndContainer)
    @Override
    public boolean getAllowEmptyListAndContainer() {
        return allowEmptyListAndContainer;
    }

    @Override
    public SubjectMap asSubjectMap() {
        return CarmlSubjectMap.builder()
                .termType(getTermType())
                .constant(getConstant())
                .reference(getReference())
                .template(getTemplate())
                .functionValue(getFunctionValue())
                .inverseExpression(getInverseExpression())
                .logicalTargets(getLogicalTargets())
                .build();
    }

    @Override
    Set<Resource> getReferencedResourcesBase() {
        return Stream.concat(super.getReferencedResourcesBase().stream(), gathers.stream())
                .collect(toUnmodifiableSet());
    }

    @Override
    void addTriplesBase(ModelBuilder builder) {
        super.addTriplesBase(builder);

        if (strategy != null) {
            builder.add(Rdf.Rml.strategy, strategy);
        }
        if (gatherAs != null) {
            builder.add(Rdf.Rml.gatherAs, gatherAs);
        }

        gathers.forEach(gather -> builder.add(Rdf.Rml.gather, gather.getAsResource()));

        builder.add(Rdf.Rml.allowEmptyListAndContainer, allowEmptyListAndContainer);
    }
}
