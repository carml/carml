package io.carml.model.impl;

import io.carml.model.Namespace;
import io.carml.model.Resource;
import io.carml.model.XmlSource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Carml;
import io.carml.vocab.Rdf;
import java.util.Objects;
import java.util.Set;
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
public class CarmlXmlSource extends CarmlSource implements XmlSource {

    @Singular
    private Set<Namespace> declaredNamespaces;

    @RdfProperty(Carml.declaresNamespace)
    @RdfType(CarmlNamespace.class)
    @Override
    public Set<Namespace> getDeclaredNamespaces() {
        return declaredNamespaces;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), declaredNamespaces);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof XmlSource other) {
            return super.equalsSource(other) && Objects.equals(declaredNamespaces, other.getDeclaredNamespaces());
        }
        return false;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.copyOf(declaredNamespaces);
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Carml.XmlDocument);
        super.addTriplesBase(modelBuilder);

        declaredNamespaces.forEach(ns -> modelBuilder.add(Carml.declaresNamespace, ns.getAsResource()));
    }
}
