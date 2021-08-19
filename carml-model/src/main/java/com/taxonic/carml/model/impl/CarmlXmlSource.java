package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Namespace;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.XmlSource;
import com.taxonic.carml.rdfmapper.annotations.RdfProperty;
import com.taxonic.carml.rdfmapper.annotations.RdfType;
import com.taxonic.carml.vocab.Carml;
import com.taxonic.carml.vocab.Rdf;
import java.util.Objects;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@NoArgsConstructor
public class CarmlXmlSource extends CarmlResource implements XmlSource {

  @Singular
  @Setter
  private Set<Namespace> declaredNamespaces;

  @RdfProperty(Carml.declaresNamespace)
  @RdfType(CarmlNamespace.class)
  @Override
  public Set<Namespace> getDeclaredNamespaces() {
    return declaredNamespaces;
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(declaredNamespaces);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof XmlSource) {
      XmlSource other = (XmlSource) obj;
      return Objects.equals(declaredNamespaces, other.getDeclaredNamespaces());
    }
    return false;
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return Set.copyOf(declaredNamespaces);
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Carml.XmlDocument);

    declaredNamespaces.forEach(ns -> modelBuilder.add(Carml.declaresNamespace, ns.getAsResource()));
  }

}
