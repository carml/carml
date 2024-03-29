package io.carml.model.impl;

import io.carml.model.FileSource;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Carml;
import io.carml.vocab.Rdf;
import java.util.Objects;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@NoArgsConstructor
public class CarmlFileSource extends CarmlResource implements FileSource {

  @Setter
  private String url;

  @RdfProperty(Carml.url)
  @Override
  public String getUrl() {
    return this.url;
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
    return Set.of();
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
