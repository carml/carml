package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
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
public class CarmlExpressionField extends CarmlExpressionMap implements ExpressionField {

    private String fieldName;

    @Singular
    private Set<Field> fields;

    @RdfProperty(Rml.fieldName)
    @Override
    public String getFieldName() {
        return fieldName;
    }

    @RdfProperty(Rml.field)
    @RdfTypeDecider(FieldTypeDecider.class)
    @Override
    public Set<Field> getFields() {
        return fields;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();
        builder.addAll(getReferencedResourcesBase());

        if (fields != null) {
            builder.addAll(fields);
        }

        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.ExpressionField);

        if (fieldName != null) {
            modelBuilder.add(Rdf.Rml.fieldName, fieldName);
        }

        addTriplesBase(modelBuilder);

        if (fields != null) {
            fields.forEach(f -> modelBuilder.add(Rdf.Rml.field, f.getAsResource()));
        }
    }
}
