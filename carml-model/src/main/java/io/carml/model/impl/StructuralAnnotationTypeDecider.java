package io.carml.model.impl;

import io.carml.rdfmapper.TypeDecider;
import io.carml.vocab.Rdf.Rml;
import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class StructuralAnnotationTypeDecider implements TypeDecider {

    @Override
    public Set<Type> decide(Model model, Resource resource) {
        // Check explicit rdf:type first
        if (model.contains(resource, RDF.TYPE, Rml.IriSafeAnnotation)) {
            return Set.of(CarmlIriSafeAnnotation.class);
        }
        if (model.contains(resource, RDF.TYPE, Rml.PrimaryKeyAnnotation)) {
            return Set.of(CarmlPrimaryKeyAnnotation.class);
        }
        if (model.contains(resource, RDF.TYPE, Rml.UniqueAnnotation)) {
            return Set.of(CarmlUniqueAnnotation.class);
        }
        if (model.contains(resource, RDF.TYPE, Rml.NotNullAnnotation)) {
            return Set.of(CarmlNotNullAnnotation.class);
        }
        if (model.contains(resource, RDF.TYPE, Rml.ForeignKeyAnnotation)) {
            return Set.of(CarmlForeignKeyAnnotation.class);
        }
        if (model.contains(resource, RDF.TYPE, Rml.InclusionAnnotation)) {
            return Set.of(CarmlInclusionAnnotation.class);
        }

        // Infer from outgoing properties: targetView indicates ForeignKey or Inclusion.
        // Without rdf:type we cannot distinguish between them, so default to ForeignKey.
        if (model.contains(resource, Rml.targetView, null)) {
            return Set.of(CarmlForeignKeyAnnotation.class);
        }

        // Simple annotations (IriSafe, PrimaryKey, Unique, NotNull) are structurally
        // identical — only rdf:type distinguishes them. Without it, we cannot infer
        // the specific type; default to a generic simple annotation.
        return Set.of(CarmlIriSafeAnnotation.class);
    }
}
