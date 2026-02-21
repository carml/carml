package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.AbstractLogicalSource;
import io.carml.model.Field;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.Resource;
import io.carml.model.StructuralAnnotation;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.rdfmapper.annotations.RdfTypeDecider;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlLogicalView extends CarmlResource implements LogicalView {

    private AbstractLogicalSource viewOn;

    @Singular
    private Set<Field> fields;

    @Singular
    private Set<LogicalViewJoin> leftJoins;

    @Singular
    private Set<LogicalViewJoin> innerJoins;

    @Singular
    private Set<StructuralAnnotation> structuralAnnotations;

    @RdfProperty(Rml.viewOn)
    @RdfTypeDecider(LogicalSourceTypeDecider.class)
    @Override
    public AbstractLogicalSource getViewOn() {
        return viewOn;
    }

    @RdfProperty(Rml.field)
    @RdfTypeDecider(FieldTypeDecider.class)
    @Override
    public Set<Field> getFields() {
        return fields;
    }

    @RdfProperty(Rml.leftJoin)
    @RdfType(CarmlLogicalViewJoin.class)
    @Override
    public Set<LogicalViewJoin> getLeftJoins() {
        return leftJoins;
    }

    @RdfProperty(Rml.innerJoin)
    @RdfType(CarmlLogicalViewJoin.class)
    @Override
    public Set<LogicalViewJoin> getInnerJoins() {
        return innerJoins;
    }

    @RdfProperty(Rml.structuralAnnotation)
    @RdfTypeDecider(StructuralAnnotationTypeDecider.class)
    @Override
    public Set<StructuralAnnotation> getStructuralAnnotations() {
        return structuralAnnotations;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();

        if (viewOn != null) {
            builder.add(viewOn);
        }
        if (fields != null) {
            builder.addAll(fields);
        }
        if (leftJoins != null) {
            builder.addAll(leftJoins);
        }
        if (innerJoins != null) {
            builder.addAll(innerJoins);
        }
        if (structuralAnnotations != null) {
            builder.addAll(structuralAnnotations);
        }

        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.LogicalView);

        if (viewOn != null) {
            modelBuilder.add(Rdf.Rml.viewOn, viewOn.getAsResource());
        }
        if (fields != null) {
            fields.forEach(f -> modelBuilder.add(Rdf.Rml.field, f.getAsResource()));
        }
        if (leftJoins != null) {
            leftJoins.forEach(lj -> modelBuilder.add(Rdf.Rml.leftJoin, lj.getAsResource()));
        }
        if (innerJoins != null) {
            innerJoins.forEach(ij -> modelBuilder.add(Rdf.Rml.innerJoin, ij.getAsResource()));
        }
        if (structuralAnnotations != null) {
            structuralAnnotations.forEach(sa -> modelBuilder.add(Rdf.Rml.structuralAnnotation, sa.getAsResource()));
        }
    }
}
