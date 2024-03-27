package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.GraphMap;
import io.carml.model.Resource;
import io.carml.model.TermType;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import java.util.function.UnaryOperator;
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
public class CarmlGraphMap extends CarmlGatherMap implements GraphMap {

    @RdfProperty(Rml.termType)
    @RdfProperty(Rr.termType)
    @Override
    public TermType getTermType() {
        if (super.getTermType() != null) {
            return super.getTermType();
        }

        return TermType.IRI;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return ImmutableSet.<Resource>builder()
                .addAll(getReferencedResourcesBase())
                .build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.GraphMap);

        addTriplesBase(modelBuilder);
    }

    @Override
    public GraphMap applyExpressionAdapter(UnaryOperator<String> referenceExpressionAdapter) {
        var graphMapBuilder = this.toBuilder();
        if (getReference() != null) {
            adaptReference(referenceExpressionAdapter, graphMapBuilder::reference);
            return graphMapBuilder.build();
        } else if (getTemplate() != null) {
            adaptTemplate(referenceExpressionAdapter, graphMapBuilder::template);
            return graphMapBuilder.build();
        } else if (getFunctionValue() != null) {
            adaptFunctionValue(referenceExpressionAdapter, graphMapBuilder::functionValue);
            return graphMapBuilder.build();
        } else {
            return this;
        }
    }
}
