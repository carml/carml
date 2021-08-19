package com.taxonic.carml.model;

import org.eclipse.rdf4j.model.Value;

public interface ExpressionMap extends Resource {

  Value getConstant();

  String getReference();

  String getTemplate();

  TriplesMap getFunctionValue();

}
