package io.carml.model;

import com.google.auto.service.AutoService;
import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

@AutoService(ReferenceFormulationTypeDecider.class)
public class JsonPathReferenceFormulationTypeDecider implements ReferenceFormulationTypeDecider {

    @Override
    public Set<Type> decide(Model model, Resource resource) {
        if (JsonPathReferenceFormulation.IRIS.contains(resource)) {
            return Set.of(JsonPathReferenceFormulation.class);
        }

        return Set.of();
    }
}
