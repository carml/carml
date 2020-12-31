package com.taxonic.carml.rdf_mapper.impl;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;

public interface ValueTransformer {

  Object transform(Model model, Value value);

}
