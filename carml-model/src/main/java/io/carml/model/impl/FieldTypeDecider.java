package io.carml.model.impl;

import io.carml.rdfmapper.TypeDecider;
import io.carml.vocab.Rdf.Fnml;
import io.carml.vocab.Rdf.Rml;
import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class FieldTypeDecider implements TypeDecider {

    @Override
    public Set<Type> decide(Model model, Resource resource) {
        // Check explicit rdf:type first
        if (model.contains(resource, RDF.TYPE, Rml.ExpressionField)) {
            return Set.of(CarmlExpressionField.class);
        }
        if (model.contains(resource, RDF.TYPE, Rml.IterableField)) {
            return Set.of(CarmlIterableField.class);
        }

        // Infer from outgoing properties: expression properties indicate ExpressionField.
        // IterableField does not have expressions — it uses an iterator (which can be implicit
        // via default iterators), so the absence of expression properties indicates IterableField.
        if (model.contains(resource, Rml.reference, null)
                || model.contains(resource, Rml.template, null)
                || model.contains(resource, Rml.constant, null)
                || model.contains(resource, Fnml.functionValue, null)
                || model.contains(resource, Rml.functionExecution, null)) {
            return Set.of(CarmlExpressionField.class);
        }

        return Set.of(CarmlIterableField.class);
    }
}
