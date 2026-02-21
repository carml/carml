package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.Field;
import io.carml.model.Resource;
import io.carml.model.StructuralAnnotation;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.util.List;
import java.util.Set;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
abstract class CarmlStructuralAnnotation extends CarmlResource implements StructuralAnnotation {

    @Singular
    private List<Field> onFields;

    @RdfProperty(Rml.onFields)
    @RdfType(CarmlExpressionField.class)
    @Override
    public List<Field> getOnFields() {
        return onFields;
    }

    Set<Resource> getReferencedResourcesBase() {
        var builder = ImmutableSet.<Resource>builder();

        if (onFields != null) {
            builder.addAll(onFields);
        }

        return builder.build();
    }

    void addTriplesBase(ModelBuilder modelBuilder) {
        if (onFields != null) {
            onFields.forEach(f -> modelBuilder.add(Rdf.Rml.onFields, f.getAsResource()));
        }
    }
}
