package com.taxonic.carml.model;

import org.eclipse.rdf4j.model.Value;

public interface TermMap extends Resource {

  String getReference();

  String getInverseExpression();

  String getTemplate();

  TermType getTermType();

  Value getConstant();

  TriplesMap getFunctionValue();

}
