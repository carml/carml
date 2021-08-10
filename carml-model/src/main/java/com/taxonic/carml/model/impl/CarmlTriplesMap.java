package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rml;
import com.taxonic.carml.vocab.Rr;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlTriplesMap extends CarmlResource implements TriplesMap {

  private LogicalSource logicalSource;

  private Set<SubjectMap> subjectMaps;

  private Set<PredicateObjectMap> predicateObjectMaps;

  public CarmlTriplesMap() {
    // Empty constructor for object mapper
  }

  public CarmlTriplesMap(LogicalSource logicalSource, Set<SubjectMap> subjectMap,
      Set<PredicateObjectMap> predicateObjectMaps) {
    this.logicalSource = logicalSource;
    this.subjectMaps = subjectMaps;
    this.predicateObjectMaps = predicateObjectMaps;
  }

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

  public void setLogicalSource(LogicalSource logicalSource) {
    this.logicalSource = logicalSource;
  }

  public void setSubjectMaps(Set<SubjectMap> subjectMaps) {
    this.subjectMaps = subjectMaps;
  }

  public void setPredicateObjectMaps(Set<PredicateObjectMap> predicateObjectMaps) {
    this.predicateObjectMaps = predicateObjectMaps;
  }

  @Override
  public String toString() {
    ToStringStyle style = new MultilineRecursiveToStringStyle();
    StringBuffer buffer = new StringBuffer();
    buffer.append(String.format("%s %s:%n ", getClass().getSimpleName(), getResourceName()));
    return new ReflectionToStringBuilder(this, style, buffer).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalSource, subjectMaps, predicateObjectMaps);
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
    CarmlTriplesMap other = (CarmlTriplesMap) obj;
    return Objects.equals(logicalSource, other.logicalSource) && Objects.equals(subjectMaps, other.subjectMaps)
        && Objects.equals(predicateObjectMaps, other.predicateObjectMaps);
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

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private LogicalSource logicalSource;

    private final Set<SubjectMap> subjectMaps = new LinkedHashSet<>();

    private final Set<PredicateObjectMap> predicateObjectMaps = new LinkedHashSet<>();

    public Builder logicalSource(LogicalSource logicalSource) {
      this.logicalSource = logicalSource;
      return this;
    }

    public Builder subjectMap(SubjectMap subjectMap) {
      subjectMaps.add(subjectMap);
      return this;
    }

    public Builder predicateObjectMap(PredicateObjectMap predicateObjectMap) {
      predicateObjectMaps.add(predicateObjectMap);
      return this;
    }

    public CarmlTriplesMap build() {
      return new CarmlTriplesMap(logicalSource, subjectMaps, predicateObjectMaps);
    }
  }

}
