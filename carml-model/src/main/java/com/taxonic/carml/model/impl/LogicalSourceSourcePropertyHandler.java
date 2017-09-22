package com.taxonic.carml.model.impl;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import javax.inject.Inject;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.PropertyHandler;
import com.taxonic.carml.rdf_mapper.impl.ComplexValueTransformer;
import com.taxonic.carml.rdf_mapper.impl.MappingCache;
import com.taxonic.carml.rdf_mapper.impl.TypeFromTripleTypeDecider;
import com.taxonic.carml.rdf_mapper.qualifiers.PropertyPredicate;
import com.taxonic.carml.rdf_mapper.qualifiers.PropertySetter;

public class LogicalSourceSourcePropertyHandler implements PropertyHandler {

	private IRI predicate;
	private BiConsumer<Object, Object> setter;
	private Mapper mapper;
	private MappingCache mappingCache;

	private Optional<Object> determineValue(Model model, Resource resource) {
		
		Set<Value> objects = model.filter(resource, predicate, null).objects();
		if (objects.size() > 1)
			throw new RuntimeException("more than 1 object for the predicate [" + predicate + "] for a logical source");
		
		if (objects.isEmpty())
			return Optional.empty();

		Value object = objects.iterator().next();
		
		if (object instanceof Literal)
			return Optional.of(object.stringValue());
		
		// map 'object' to some complex type
		// TODO quite nasty to create the transformer here
		ComplexValueTransformer transformer = new ComplexValueTransformer(
			new TypeFromTripleTypeDecider(),
			mappingCache,
			mapper,
			o -> o
		);
		Object value = transformer.transform(model, object);
		return Optional.of(value);
	}
	
	@Override
	public void handle(Model model, Resource resource, Object instance) {
		
		determineValue(model, resource)
		
		.ifPresent(value ->
			setter.accept(instance, value)
		);
	}

	@Inject @PropertyPredicate
	public void setPredicate(IRI predicate) {
		this.predicate = predicate;
	}
	
	@Inject @PropertySetter
	public void setSetter(BiConsumer<Object, Object> setter) {
		this.setter = setter;
	}
	
	@Inject
	public void setMapper(Mapper mapper) {
		this.mapper = mapper;
	}
	
	@Inject
	public void setMappingCache(MappingCache mappingCache) {
		this.mappingCache = mappingCache;
	}
	
}
