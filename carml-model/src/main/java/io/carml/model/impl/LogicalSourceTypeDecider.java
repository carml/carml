package io.carml.model.impl;

import io.carml.rdfmapper.TypeDecider;
import io.carml.vocab.Rdf.Rml;
import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class LogicalSourceTypeDecider implements TypeDecider {

    @Override
    public Set<Type> decide(Model model, Resource resource) {
        if (model.contains(resource, RDF.TYPE, Rml.LogicalView)) {
            return Set.of(CarmlLogicalView.class);
        }
        if (model.contains(resource, Rml.viewOn, null)
                || model.contains(resource, Rml.field, null)
                || model.contains(resource, Rml.leftJoin, null)
                || model.contains(resource, Rml.innerJoin, null)
                || model.contains(resource, Rml.structuralAnnotation, null)) {
            return Set.of(CarmlLogicalView.class);
        }
        return Set.of(CarmlLogicalSource.class);
    }
}
