package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdfmapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rml;
import java.util.Objects;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@NoArgsConstructor
public class CarmlLogicalSource extends CarmlResource implements LogicalSource {

  @Setter
  private Object source;

  @Setter
  private String iterator;

  @Setter
  private IRI referenceFormulation;

  @RdfProperty(value = Rml.source, handler = LogicalSourceSourcePropertyHandler.class)
  @Override
  public Object getSource() {
    return source;
  }

  @RdfProperty(Rml.iterator)
  @Override
  public String getIterator() {
    return iterator;
  }

  @RdfProperty(Rml.referenceFormulation)
  @Override
  public IRI getReferenceFormulation() {
    return referenceFormulation;
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, iterator, referenceFormulation);
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
    CarmlLogicalSource other = (CarmlLogicalSource) obj;
    return Objects.equals(source, other.source) && Objects.equals(iterator, other.iterator)
        && Objects.equals(referenceFormulation, other.referenceFormulation);
  }

  @Override
  public Set<Resource> getReferencedResources() {
    if (source instanceof Resource) {
      return Set.of((Resource) source);
    } else {
      return Set.of();
    }
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rml.LogicalSource);
    if (source != null) {
      if (source instanceof Resource) {
        modelBuilder.add(Rml.source, ((Resource) source).getAsResource());
      } else {
        modelBuilder.add(Rml.source, source);
      }
    }
    if (iterator != null) {
      modelBuilder.add(Rml.iterator, iterator);
    }
    if (referenceFormulation != null) {
      modelBuilder.add(Rml.referenceFormulation, referenceFormulation);
    }
  }

}
