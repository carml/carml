package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.Field;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfTypeDecider;
import io.carml.vocab.Rml;
import java.util.Set;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
abstract class CarmlField extends CarmlResource implements Field {

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

        if (fields != null) {
            builder.addAll(fields);
        }

        return builder.build();
    }

    @Override
    public void addTriples(org.eclipse.rdf4j.model.util.ModelBuilder modelBuilder) {
        if (fieldName != null) {
            modelBuilder.add(io.carml.vocab.Rdf.Rml.fieldName, fieldName);
        }
        if (fields != null) {
            fields.forEach(f -> modelBuilder.add(io.carml.vocab.Rdf.Rml.field, f.getAsResource()));
        }
    }
}
