package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.Condition;
import io.carml.model.FunctionExecution;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
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

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlCondition extends CarmlResource implements Condition {

    private FunctionExecution functionExecution;

    private String isNull;

    private String isNotNull;

    @Singular("equalsValue")
    private Set<String> equals;

    @Singular("notEqualsValue")
    private Set<String> notEquals;

    @RdfProperty(Rml.functionExecution)
    @RdfType(CarmlFunctionExecution.class)
    @Override
    public FunctionExecution getFunctionExecution() {
        return functionExecution;
    }

    @RdfProperty(Rml.isNull)
    @Override
    public String getIsNull() {
        return isNull;
    }

    @RdfProperty(Rml.isNotNull)
    @Override
    public String getIsNotNull() {
        return isNotNull;
    }

    @RdfProperty(Rml.equals)
    @Override
    public Set<String> getEquals() {
        return equals != null ? equals : Set.of();
    }

    @RdfProperty(Rml.notEquals)
    @Override
    public Set<String> getNotEquals() {
        return notEquals != null ? notEquals : Set.of();
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();
        if (functionExecution != null) {
            builder.add(functionExecution);
        }
        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource());
        if (functionExecution != null) {
            modelBuilder.add(Rdf.Rml.functionExecution, functionExecution.getAsResource());
        }
        if (isNull != null) {
            modelBuilder.add(Rdf.Rml.isNull, isNull);
        }
        if (isNotNull != null) {
            modelBuilder.add(Rdf.Rml.isNotNull, isNotNull);
        }
        if (equals != null) {
            equals.forEach(v -> modelBuilder.add(Rdf.Rml.equals, v));
        }
        if (notEquals != null) {
            notEquals.forEach(v -> modelBuilder.add(Rdf.Rml.notEquals, v));
        }
    }
}
