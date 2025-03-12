package io.carml.model;

import io.carml.model.impl.CarmlResource;
import io.carml.rdfmapper.annotations.RdfProperty;
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
public class XmlNamespace extends CarmlResource {

    private String namespacePrefix;

    private String namespaceUrl;

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.Namespace);

        if (namespacePrefix != null) {
            modelBuilder.subject(getAsResource()).add(Rdf.Rml.namespacePrefix, namespacePrefix);
        }

        if (namespaceUrl != null) {
            modelBuilder.subject(getAsResource()).add(Rdf.Rml.namespaceURL, namespaceUrl);
        }
    }

    @RdfProperty(Rml.namespacePrefix)
    public String getNamespacePrefix() {
        return namespacePrefix;
    }

    @RdfProperty(Rml.namespaceURL)
    public String getNamespaceUrl() {
        return namespaceUrl;
    }
}
