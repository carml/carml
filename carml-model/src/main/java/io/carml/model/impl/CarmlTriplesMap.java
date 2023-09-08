package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.LogicalSource;
import io.carml.model.PredicateObjectMap;
import io.carml.model.Resource;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class CarmlTriplesMap extends CarmlResource implements TriplesMap {

  @Setter
  private LogicalSource logicalSource;

  @Singular
  @Setter
  private Set<SubjectMap> subjectMaps;

  @Singular
  @Setter
  private Set<PredicateObjectMap> predicateObjectMaps;

  @RdfProperty(Rml.logicalSource)
  @RdfType(CarmlLogicalSource.class)
  @Override
  public LogicalSource getLogicalSource() {
    return logicalSource;
  }

  @RdfProperty(Rr.subjectMap)
  @RdfType(CarmlSubjectMap.class)
  @Override
  public Set<SubjectMap> getSubjectMaps() {
    return subjectMaps;
  }

  @RdfProperty(Rr.predicateObjectMap)
  @RdfType(CarmlPredicateObjectMap.class)
  @Override
  public Set<PredicateObjectMap> getPredicateObjectMaps() {
    return predicateObjectMaps;
  }

  @SuppressWarnings("java:S1149")
  @Override
  public String toString() {
    ToStringStyle style = new MultilineRecursiveToStringStyle();
    StringBuffer buffer = new StringBuffer();
    buffer.append(String.format("%s %s:%n ", getClass().getSimpleName(), getResourceName()));
    return new ReflectionToStringBuilder(this, style, buffer).toString();
  }

  @Override
  public Set<Resource> getReferencedResources() {
    ImmutableSet.Builder<Resource> builder = ImmutableSet.<Resource>builder();
    if (logicalSource != null) {
      builder.add(logicalSource);
    }
    return builder.addAll(subjectMaps)
        .addAll(predicateObjectMaps)
        .build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rr.TriplesMap);
    if (logicalSource != null) {
      modelBuilder.add(Rml.logicalSource, logicalSource.getAsResource());
    }
    subjectMaps.forEach(sm -> modelBuilder.add(Rr.subjectMap, sm.getAsResource()));
    predicateObjectMaps.forEach(pom -> modelBuilder.add(Rr.predicateObjectMap, pom.getAsResource()));
  }
}
