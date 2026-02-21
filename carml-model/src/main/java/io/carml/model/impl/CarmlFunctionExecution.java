package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.FunctionExecution;
import io.carml.model.FunctionMap;
import io.carml.model.Input;
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
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlFunctionExecution extends CarmlResource implements FunctionExecution {

    private FunctionMap functionMap;

    @Singular
    private Set<Input> inputs;

    @RdfProperty(Rml.functionMap)
    @RdfProperty(Rml.function)
    @RdfType(CarmlFunctionMap.class)
    @Override
    public FunctionMap getFunctionMap() {
        return functionMap;
    }

    @RdfProperty(Rml.input)
    @RdfType(CarmlInput.class)
    @Override
    public Set<Input> getInputs() {
        return inputs;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();

        if (functionMap != null) {
            builder.add(functionMap);
        }
        if (inputs != null) {
            builder.addAll(inputs);
        }

        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.FunctionExecution);

        if (functionMap != null) {
            modelBuilder.add(Rdf.Rml.functionMap, functionMap.getAsResource());
        }
        if (inputs != null) {
            inputs.forEach(i -> modelBuilder.add(Rdf.Rml.input, i.getAsResource()));
        }
    }
}
