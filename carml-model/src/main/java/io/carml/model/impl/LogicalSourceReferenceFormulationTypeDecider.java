package io.carml.model.impl;

import io.carml.model.ReferenceFormulationTypeDecider;
import io.carml.rdfmapper.TypeDecider;
import io.carml.rdfmapper.impl.CarmlMapperException;
import java.lang.reflect.Type;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public class LogicalSourceReferenceFormulationTypeDecider implements TypeDecider {

    @Override
    public Set<Type> decide(Model model, Resource resource) {
        var types = ServiceLoader.load(ReferenceFormulationTypeDecider.class).stream()
                .map(ServiceLoader.Provider::get)
                .map(delegate -> delegate.decide(model, resource))
                .flatMap(Set::stream)
                .collect(Collectors.toUnmodifiableSet());

        if (types.isEmpty()) {
            throw new CarmlMapperException(
                    String.format("Encountered unsupported reference formulation: %s", resource));
        }

        return types;
    }
}
