package com.taxonic.carml.rdf_mapper.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;

interface DependencyResolver {

  Object resolve(Type type, List<Annotation> qualifiers);

}
