package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.FileSource;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;
import com.taxonic.carml.vocab.Rdf;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlFileSource extends CarmlResource implements FileSource {

  private String url;

  public CarmlFileSource() {
    // Empty constructor for object mapper
  }

  @RdfProperty(Carml.url)
  @Override
  public String getUrl() {
    return this.url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(url);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FileSource) {
      FileSource other = (FileSource) obj;
      return Objects.equals(url, other.getUrl());
    }
    return false;
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return ImmutableSet.of();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Carml.FileSource);

    if (url != null) {
      modelBuilder.add(Carml.url, url);
    }
  }
}
