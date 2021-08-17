package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdfmapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rr;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlObjectMap extends CarmlTermMap implements ObjectMap {

  private IRI datatype;

  private String language;

  public CarmlObjectMap() {
    // Empty constructor for object mapper
  }

  public CarmlObjectMap(String reference, String inverseExpression, String template, TermType termType, Value constant,
      TriplesMap functionValue, IRI datatype, String language) {
    super(reference, inverseExpression, template, termType, constant, functionValue);
    this.datatype = datatype;
    this.language = language;
  }

  @RdfProperty(Rr.datatype)
  @Override
  public IRI getDatatype() {
    return datatype;
  }

  @RdfProperty(Rr.language)
  @Override
  public String getLanguage() {
    return language;
  }

  public void setDatatype(IRI datatype) {
    this.datatype = datatype;
  }

  public void setLanguage(String language) {
    this.language = language;
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(datatype, language, super.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CarmlObjectMap other = (CarmlObjectMap) obj;
    return Objects.equals(datatype, other.datatype) && Objects.equals(language, other.language);
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
        .add(RDF.TYPE, Rdf.Rr.ObjectMap);

    addTriplesBase(modelBuilder);

    if (datatype != null) {
      modelBuilder.add(Rr.datatype, datatype);
    }
    if (language != null) {
      modelBuilder.add(Rr.language, language);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends CarmlTermMap.Builder {

    private IRI datatype;

    private String language;

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

    public Builder datatype(IRI datatype) {
      this.datatype = datatype;
      return this;
    }

    public Builder language(String language) {
      this.language = language;
      return this;
    }

    public CarmlObjectMap build() {
      return new CarmlObjectMap(getReference(), getInverseExpression(), getTemplate(), getTermType(), getConstant(),
          getFunctionValue(), datatype, language);
    }
  }
}
