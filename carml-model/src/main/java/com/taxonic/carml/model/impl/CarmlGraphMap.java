package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.vocab.Rdf;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlGraphMap extends CarmlTermMap implements GraphMap {

  public CarmlGraphMap() {
    // Empty constructor for object mapper
  }

  public CarmlGraphMap(String reference, String inverseExpression, String template, TermType termType, Value constant,
      TriplesMap functionValue) {
    super(reference, inverseExpression, template, termType, constant, functionValue);
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return ImmutableSet.<Resource>builder()
        .addAll(getReferencedResourcesBase())
        .build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rr.GraphMap);

    addTriplesBase(modelBuilder);
  }


  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends CarmlTermMap.Builder {

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

    public CarmlGraphMap build() {
      return new CarmlGraphMap(getReference(), getInverseExpression(), getTemplate(), getTermType(), getConstant(),
          getFunctionValue());
    }
  }
}
