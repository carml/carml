package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.LogicalView;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.util.List;
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
public class CarmlForeignKeyAnnotation extends CarmlStructuralAnnotation implements ForeignKeyAnnotation {

    private LogicalView targetView;

    @Singular
    private List<Field> targetFields;

    @RdfProperty(Rml.targetView)
    @RdfType(CarmlLogicalView.class)
    @Override
    public LogicalView getTargetView() {
        return targetView;
    }

    @RdfProperty(Rml.targetFields)
    @RdfType(CarmlExpressionField.class)
    @Override
    public List<Field> getTargetFields() {
        return targetFields;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();
        builder.addAll(getReferencedResourcesBase());

        if (targetView != null) {
            builder.add(targetView);
        }
        if (targetFields != null) {
            builder.addAll(targetFields);
        }

        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.ForeignKeyAnnotation);

        addTriplesBase(modelBuilder);

        if (targetView != null) {
            modelBuilder.add(Rdf.Rml.targetView, targetView.getAsResource());
        }
        if (targetFields != null) {
            targetFields.forEach(f -> modelBuilder.add(Rdf.Rml.targetFields, f.getAsResource()));
        }
    }
}
