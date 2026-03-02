package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.IterableField;
import io.carml.model.ReferenceFormulation;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfTypeDecider;
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
public class CarmlIterableField extends CarmlField implements IterableField {

    private ReferenceFormulation referenceFormulation;

    private String iterator;

    @RdfProperty(Rml.referenceFormulation)
    @RdfTypeDecider(LogicalSourceReferenceFormulationTypeDecider.class)
    @Override
    public ReferenceFormulation getReferenceFormulation() {
        return referenceFormulation;
    }

    @RdfProperty(Rml.iterator)
    @Override
    public String getIterator() {
        return iterator;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();

        if (getFields() != null) {
            builder.addAll(getFields());
        }

        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.IterableField);

        if (getFieldName() != null) {
            modelBuilder.add(Rdf.Rml.fieldName, getFieldName());
        }
        if (iterator != null) {
            modelBuilder.add(Rdf.Rml.iterator, iterator);
        }
        if (referenceFormulation != null) {
            modelBuilder.add(Rdf.Rml.referenceFormulation, referenceFormulation.getAsResource());
        }
        if (getFields() != null) {
            getFields().forEach(f -> modelBuilder.add(Rdf.Rml.field, f.getAsResource()));
        }
    }
}
