package io.carml.model;

import io.carml.model.impl.CarmlResource;
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
public class XPathReferenceFormulation extends CarmlResource implements ReferenceFormulation {

    public static final Set<org.eclipse.rdf4j.model.Resource> IRIS = Set.of(Rdf.Rml.XPath, Rdf.Ql.XPath);

    @Singular
    private Set<XmlNamespace> namespaces;

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.copyOf(namespaces);
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.XPathReferenceFormulation);

        namespaces.forEach(
                namespace -> modelBuilder.subject(getAsResource()).add(Rml.namespace, namespace.getAsResource()));
    }

    @RdfProperty(Rml.namespace)
    @RdfType(XmlNamespace.class)
    public Set<XmlNamespace> getNamespaces() {
        return namespaces;
    }
}
