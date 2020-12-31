package com.taxonic.carml.engine.function;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

interface ExtractParameter {

  Object extract(Model model, Resource subject);

}
