package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Namespace;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdfmapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;
import com.taxonic.carml.vocab.Rdf;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlNamespace extends CarmlResource implements Namespace {

  private String prefix;

  private String name;

  public CarmlNamespace() {
    // Empty constructor for object mapper
  }

  public CarmlNamespace(String prefix, String name) {
    this.prefix = prefix;
    this.name = name;
  }

  @RdfProperty(Carml.namespacePrefix)
  @Override
  public String getPrefix() {
    return prefix;
  }

  @RdfProperty(Carml.namespaceName)
  @Override
  public String getName() {
    return name;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefix, name);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CarmlNamespace other = (CarmlNamespace) obj;
    return Objects.equals(prefix, other.prefix) && Objects.equals(name, other.name);
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return Set.of();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Carml.Namespace);
    if (prefix != null) {
      modelBuilder.add(Carml.namespacePrefix, prefix);
    }
    if (name != null) {
      modelBuilder.add(Carml.namespaceName, name);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String prefix;

    private String name;

    Builder() {}

    public Builder prefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public CarmlNamespace build() {
      return new CarmlNamespace(prefix, name);
    }
  }

}
