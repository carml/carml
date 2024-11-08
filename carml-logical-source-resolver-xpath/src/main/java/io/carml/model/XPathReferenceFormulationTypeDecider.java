package io.carml.model;

import com.google.auto.service.AutoService;
import io.carml.vocab.Rdf.Rml;
import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@AutoService(ReferenceFormulationTypeDecider.class)
public class XPathReferenceFormulationTypeDecider implements ReferenceFormulationTypeDecider {

    @Override
    public Set<Type> decide(Model model, Resource resource) {
        if (XPathReferenceFormulation.IRIS.contains(resource)) {
            return Set.of(XPathReferenceFormulation.class);
        }

        if (model.contains(resource, RDF.TYPE, Rml.XPathReferenceFormulation)) {
            return Set.of(XPathReferenceFormulation.class);
        }

        if (model.contains(resource, Rml.namespace, null)) {
            return Set.of(XPathReferenceFormulation.class);
        }

        return Set.of();
    }
}
