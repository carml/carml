package io.carml.model;

import com.google.auto.service.AutoService;
import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

@AutoService(ReferenceFormulationTypeDecider.class)
public class SqlReferenceFormulationTypeDecider implements ReferenceFormulationTypeDecider {

    @Override
    public Set<Type> decide(Model model, Resource resource) {
        // TODO: can we handle legacy R2RML constructs here?
        if (SqlReferenceFormulation.IRIS.contains(resource)) {
            return Set.of(SqlReferenceFormulation.class);
        }

        return Set.of();
    }
}
