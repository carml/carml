package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.ExpressionMap;
import io.carml.model.Input;
import io.carml.model.ParameterMap;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
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
public class CarmlInput extends CarmlResource implements Input {

    private ParameterMap parameterMap;

    private ExpressionMap inputValueMap;

    @RdfProperty(Rml.parameterMap)
    @RdfType(CarmlParameterMap.class)
    @Override
    public ParameterMap getParameterMap() {
        return parameterMap;
    }

    @RdfProperty(Rml.inputValueMap)
    @RdfType(CarmlObjectMap.class)
    @Override
    public ExpressionMap getInputValueMap() {
        return inputValueMap;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();

        if (parameterMap != null) {
            builder.add(parameterMap);
        }
        if (inputValueMap != null) {
            builder.add(inputValueMap);
        }

        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.Input);

        if (parameterMap != null) {
            modelBuilder.add(Rdf.Rml.parameterMap, parameterMap.getAsResource());
        }
        if (inputValueMap != null) {
            modelBuilder.add(Rdf.Rml.inputValueMap, inputValueMap.getAsResource());
        }
    }
}
