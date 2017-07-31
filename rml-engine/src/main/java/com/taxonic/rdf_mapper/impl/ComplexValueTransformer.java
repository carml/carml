package com.taxonic.rdf_mapper.impl;

import java.lang.reflect.Type;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

import com.taxonic.rdf_mapper.Mapper;

class ComplexValueTransformer implements ValueTransformer {

	private TypeDecider typeDecider;
	private MappingCache mappingCache;
	private Mapper mapper;
	private Function<Object, Object> typeAdapter;
	
	public ComplexValueTransformer(
		TypeDecider typeDecider,
		MappingCache mappingCache,
		Mapper mapper,
		Function<Object, Object> typeAdapter
	) {
		this.typeDecider = typeDecider;
		this.mappingCache = mappingCache;
		this.mapper = mapper;
		this.typeAdapter = typeAdapter;
	}

	@Override
	public Object transform(Model model, Value value) {
	
		Resource resource = (Resource) value;
		
		// determine exact target type
		Type targetType = typeDecider.decide(model, resource);
		
		// TODO check for target type conditions?
		// - must be a subtype of 'propertyType'
		// - must be a specific type, eg. no unbound type parameters, not an interface
		
		Object targetValue;
		
		// before mapping, first check the cache for an existing mapping
		// NOTE: cache includes pre-mapped/registered enum instances
		// such as <#Male> -> Gender.Male for property gender : Gender
		targetValue = mappingCache.getCachedMapping(resource, targetType);
		
		if (targetValue == null) {
			
			// no existing mapping - perform mapping
			targetValue = mapper.map(model, resource, targetType);
			
			// add mapped value to cache
			mappingCache.addCachedMapping(resource, targetType, targetValue);
		}
		
		// TODO check cache for adapted value (key: typeAdapter + targetValue)
		Object adaptedValue = typeAdapter.apply(targetValue);
		// TODO maybe we should cache this as well, in a diff. cache. (key: typeAdapter + targetValue)
		
		return adaptedValue;		
	}
}
