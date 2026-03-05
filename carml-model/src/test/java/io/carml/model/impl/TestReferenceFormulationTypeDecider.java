package io.carml.model.impl;

import com.google.auto.service.AutoService;
import io.carml.model.ReferenceFormulationTypeDecider;
import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

@AutoService(ReferenceFormulationTypeDecider.class)
public class TestReferenceFormulationTypeDecider implements ReferenceFormulationTypeDecider {

    @Override
    public Set<Type> decide(Model model, Resource resource) {
        if (SqlReferenceFormulation.IRIS.contains(resource)) {
            return Set.of();
        }
        return Set.of(TestReferenceFormulation.class);
    }
}
