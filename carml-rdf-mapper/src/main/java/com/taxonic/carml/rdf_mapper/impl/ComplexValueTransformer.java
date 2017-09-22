package com.taxonic.carml.rdf_mapper.impl;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.TypeDecider;

public class ComplexValueTransformer implements ValueTransformer {

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
	
	private static final Map<IRI, Function<Literal, Object>> literalGetters;
	
	static {
		
		// TODO umm, somewhat convoluted
		class CreateLiteralGetters {
			Map<IRI, Function<Literal, Object>> getters = new LinkedHashMap<>();
			
			void add(IRI type, Function<Literal, Object> getter) {
				getters.put(type, getter);
			}
			
			Map<IRI, Function<Literal, Object>> run() {
				add(XMLSchema.BOOLEAN, Literal::booleanValue);
				add(XMLSchema.STRING, Literal::getLabel);
				add(XMLSchema.DECIMAL, Literal::decimalValue);
				add(XMLSchema.FLOAT, Literal::floatValue);
				add(XMLSchema.INT, Literal::intValue);
				add(XMLSchema.INTEGER, Literal::integerValue); // BigInteger
				add(XMLSchema.DOUBLE, Literal::doubleValue);
				// TODO more types, most notably xsd:date and variations
				return Collections.unmodifiableMap(getters);
			}
		}
		literalGetters = new CreateLiteralGetters().run();
	}

	private Object transform(Literal literal) {
		IRI type = literal.getDatatype();
		Function<Literal, Object> getter = literalGetters.get(type);
		if (getter == null)
			throw new RuntimeException("no getter for Literal defined that can handle literal with datatype [" + type + "]");
		return getter.apply(literal);
	}
	
	@Override
	public Object transform(Model model, Value value) {
	
		if (value instanceof Literal)
			return transform((Literal) value);
		
		
		// =========== RESOURCE ===========
		
		
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
