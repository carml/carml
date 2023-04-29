package io.carml.model.impl;

import io.carml.model.ExpressionMap;
import io.carml.model.Resource;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Carml;
import io.carml.vocab.Fnml;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuppressWarnings("java:S1135")
@SuperBuilder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Setter
@ToString(callSuper = true)

abstract class CarmlExpressionMap extends CarmlResource implements ExpressionMap {

  CarmlExpressionMap(String id, String label, String reference, String template, Value constant,
      TriplesMap functionValue) {
    super(id, label);
    this.reference = reference;
    this.template = template;
    this.constant = constant;
    this.functionValue = functionValue;
  }

  String reference;

  String template;

  Value constant;

  TriplesMap functionValue;

  @RdfProperty(Rml.reference)
  @RdfProperty(value = Carml.multiReference, deprecated = true)
  @Override
  public String getReference() {
    return reference;
  }

  @RdfProperty(Rr.template)
  @RdfProperty(value = Carml.multiTemplate, deprecated = true)
  @Override
  public String getTemplate() {
    return template;
  }

  @RdfProperty(Rr.constant)
  @Override
  public Value getConstant() {
    return constant;
  }

  @RdfProperty(Fnml.functionValue)
  @RdfProperty(value = Carml.multiFunctionValue, deprecated = true)
  @RdfType(CarmlTriplesMap.class)
  @Override
  public TriplesMap getFunctionValue() {
    return functionValue;
  }

  Set<Resource> getReferencedResourcesBase() {
    return functionValue != null ? Set.of(functionValue) : Set.of();
  }

  void addTriplesBase(ModelBuilder builder) {
    if (reference != null) {
      builder.add(Rml.reference, reference);
    }
    if (template != null) {
      builder.add(Rr.template, template);
    }
    if (constant != null) {
      builder.add(Rr.constant, constant);
    }
    if (functionValue != null) {
      builder.add(Fnml.functionValue, functionValue.getAsResource());
    }
  }
}
