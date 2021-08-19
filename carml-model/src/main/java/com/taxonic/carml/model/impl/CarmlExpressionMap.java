package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.ExpressionMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdfmapper.annotations.RdfProperty;
import com.taxonic.carml.rdfmapper.annotations.RdfType;
import com.taxonic.carml.vocab.Carml;
import com.taxonic.carml.vocab.Fnml;
import com.taxonic.carml.vocab.Rml;
import com.taxonic.carml.vocab.Rr;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PACKAGE)
abstract class CarmlExpressionMap extends CarmlResource implements ExpressionMap {

  CarmlExpressionMap(String id, String label, String reference, String template, Value constant,
      TriplesMap functionValue) {
    super(id, label);
    this.reference = reference;
    this.template = template;
    this.constant = constant;
    this.functionValue = functionValue;
  }

  @Setter
  String reference;

  @Setter
  String template;

  // TODO constant could also be a lang string or maybe something else.
  @Setter
  Value constant;

  @Setter
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
