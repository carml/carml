package io.carml.model.impl;

import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.model.LogicalTarget;
import io.carml.model.Target;
import io.carml.model.TermMap;
import io.carml.model.TermType;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
abstract class CarmlTermMap extends CarmlExpressionMap implements TermMap {

    private String inverseExpression;

    @Getter
    private TermType termType;

    @Default
    private Set<LogicalTarget> logicalTargets = Set.of();

    // TODO??
    @RdfProperty(Rml.inverseExpression)
    @RdfProperty(Rr.inverseExpression)
    @Override
    public String getInverseExpression() {
        return inverseExpression;
    }

    @RdfProperty(Rml.logicalTarget)
    @Override
    public Set<LogicalTarget> getLogicalTargets() {
        return logicalTargets;
    }

    public Set<Target> getTargets() {
        return logicalTargets.stream() //
                .map(LogicalTarget::getTarget)
                .collect(toUnmodifiableSet());
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
