package io.carml.model.impl;

import io.carml.model.GatherMap;
import io.carml.model.ObjectMap;
import io.carml.model.Strategy;
import io.carml.model.TermMap;
import io.carml.model.TermType;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.List;
import lombok.AccessLevel;
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
abstract class CarmlTermMap extends CarmlExpressionMap implements TermMap, GatherMap {

    String inverseExpression;

    TermType termType;

    Strategy strategy;

    IRI gatherAs;

    List<ObjectMap> gatheredOnes;

    boolean allowEmptyListAndContainer;

    @RdfProperty(Rml.inverseExpression)
    @RdfProperty(Rr.inverseExpression)
    @Override
    public String getInverseExpression() {
        return inverseExpression;
    }

    @Override
    public TermType getTermType() {
        return termType;
    }

    @Override
    @RdfProperty(Rml.strategy)
    public Strategy getStrategy() {
        return strategy;
    }

    @Override
    @RdfProperty(Rml.gatherAs)
    public IRI getGatherAs() {
        return gatherAs;
    }

    @Override
    @RdfProperty(Rml.gather)
    @RdfType(CarmlObjectMap.class)
    public List<ObjectMap> getGatheredOnes() {
        return gatheredOnes;
    }

    @Override
    public boolean getAllowEmptyListAndContainer() {
        return allowEmptyListAndContainer;
    }

    @Override
    void addTriplesBase(ModelBuilder builder) {
        super.addTriplesBase(builder);

        if (inverseExpression != null) {
            builder.add(Rdf.Rml.inverseExpression, inverseExpression);
        }
        if (termType != null) {
            addTermTypeTriple(builder);
        }
    }

    private void addTermTypeTriple(ModelBuilder builder) {
        switch (termType) {
            case IRI:
                builder.add(Rdf.Rml.termType, Rdf.Rml.IRI);
                break;
            case LITERAL:
                builder.add(Rdf.Rml.termType, Rdf.Rml.Literal);
                break;
            case BLANK_NODE:
                builder.add(Rdf.Rml.termType, Rdf.Rml.BlankNode);
                break;
            default:
                throw new IllegalStateException(String.format("Illegal term type value '%s' encountered.", termType));
        }
    }
}
