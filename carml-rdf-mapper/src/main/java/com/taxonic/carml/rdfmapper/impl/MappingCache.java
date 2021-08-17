package com.taxonic.carml.rdf_mapper.impl;

import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;

public interface MappingCache {

  Object getCachedMapping(Resource resource, Set<Type> targetType);

  void addCachedMapping(Resource resource, Set<Type> targetType, Object value);

}
