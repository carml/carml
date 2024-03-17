package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.GraphMap;
import io.carml.model.Resource;
import io.carml.model.SubjectMap;
import io.carml.model.TermType;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import java.util.function.UnaryOperator;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CarmlSubjectMap extends CarmlTermMap implements SubjectMap {

    @Singular("clazz")
    private Set<IRI> classes;

    @Singular
    private Set<GraphMap> graphMaps;

    @RdfProperty(Rml.graphMap)
    @RdfProperty(Rr.graphMap)
    @RdfType(CarmlGraphMap.class)
    @Override
    public Set<GraphMap> getGraphMaps() {
        return graphMaps;
    }

    @RdfProperty(Rml.clazz)
    @RdfProperty(Rr.clazz)
    @Override
    public Set<IRI> getClasses() {
        return classes;
    }

    @RdfProperty(Rml.termType)
    @RdfProperty(Rr.termType)
    @Override
    public TermType getTermType() {
        if (termType != null) {
            return termType;
        }

        return TermType.IRI;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return ImmutableSet.<Resource>builder()
                .addAll(getReferencedResourcesBase())
                .addAll(graphMaps)
                .build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.SubjectMap);

        addTriplesBase(modelBuilder);

        graphMaps.forEach(gm -> modelBuilder.add(Rdf.Rml.graphMap, gm.getAsResource()));
        classes.forEach(cl -> modelBuilder.add(Rdf.Rml.clazz, cl));
    }

    @Override
    public SubjectMap applyExpressionAdapter(UnaryOperator<String> referenceExpressionAdapter) {
        var subjectMapBuilder = this.toBuilder();
        if (reference != null) {
            adaptReference(referenceExpressionAdapter, subjectMapBuilder::reference);
            return subjectMapBuilder.build();
        } else if (template != null) {
            adaptTemplate(referenceExpressionAdapter, subjectMapBuilder::template);
            return subjectMapBuilder.build();
        } else if (functionValue != null) {
            adaptFunctionValue(referenceExpressionAdapter, subjectMapBuilder::functionValue);
            return subjectMapBuilder.build();
        } else {
            return this;
        }
    }
}
