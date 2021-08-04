package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rr;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlSubjectMap extends CarmlTermMap implements SubjectMap {

  private Set<IRI> classes;

  private Set<GraphMap> graphMaps;

  public CarmlSubjectMap() {
    // Empty constructor for object mapper
  }

  public CarmlSubjectMap(String reference, String inverseExpression, String template, TermType termType, Value constant,
      TriplesMap functionValue, Set<IRI> classes, Set<GraphMap> graphMaps) {
    super(reference, inverseExpression, template, termType, constant, functionValue);
    this.classes = classes;
    this.graphMaps = graphMaps;
  }


  @RdfProperty(Rr.graphMap)
  @RdfType(CarmlGraphMap.class)
  @Override
  public Set<GraphMap> getGraphMaps() {
    return graphMaps;
  }

  public void setGraphMaps(Set<GraphMap> graphMaps) {
    this.graphMaps = graphMaps;
  }

  @RdfProperty(Rr.clazz)
  @Override
  public Set<IRI> getClasses() {
    return classes;
  }

  public void setClasses(Set<IRI> classes) {
    this.classes = classes;
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return ImmutableSet.<Resource>builder()
        .addAll(getReferencedResourcesBase())
        .addAll(graphMaps)
        .build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rr.SubjectMap);

    addTriplesBase(modelBuilder);

    graphMaps.forEach(gm -> modelBuilder.add(Rr.graphMap, gm.getAsResource()));
    classes.forEach(cl -> modelBuilder.add(Rr.clazz, cl));
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends CarmlTermMap.Builder {

    private Set<IRI> classes = new LinkedHashSet<>();

    private Set<GraphMap> graphMaps = new LinkedHashSet<>();

    @Override
    public Builder reference(String reference) {
      super.reference(reference);
      return this;
    }

    @Override
    public Builder inverseExpression(String inverseExpression) {
      super.inverseExpression(inverseExpression);
      return this;
    }

    @Override
    public Builder template(String template) {
      super.template(template);
      return this;
    }

    @Override
    public Builder termType(TermType termType) {
      super.termType(termType);
      return this;
    }

    @Override
    public Builder constant(Value constant) {
      super.constant(constant);
      return this;
    }

    @Override
    public Builder functionValue(TriplesMap functionValue) {
      super.functionValue(functionValue);
      return this;
    }

    public Builder clazz(IRI clazz) {
      classes.add(clazz);
      return this;
    }

    public Builder classes(Set<IRI> classes) {
      this.classes = classes;
      return this;
    }

    public Builder graphMap(CarmlGraphMap carmlGraphMap) {
      graphMaps.add(carmlGraphMap);
      return this;
    }

    public Builder graphMaps(Set<GraphMap> graphMaps) {
      this.graphMaps = graphMaps;
      return this;
    }

    public CarmlSubjectMap build() {
      return new CarmlSubjectMap(getReference(), getInverseExpression(), getTemplate(), getTermType(), getConstant(),
          getFunctionValue(), classes, graphMaps);
    }
  }
}
