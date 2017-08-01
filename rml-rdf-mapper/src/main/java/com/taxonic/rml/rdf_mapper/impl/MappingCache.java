package com.taxonic.rml.rdf_mapper.impl;

import java.lang.reflect.Type;

import org.eclipse.rdf4j.model.Resource;

public interface MappingCache {

	Object getCachedMapping(Resource resource, Type targetType);

	void addCachedMapping(Resource resource, Type targetType, Object value);

}
