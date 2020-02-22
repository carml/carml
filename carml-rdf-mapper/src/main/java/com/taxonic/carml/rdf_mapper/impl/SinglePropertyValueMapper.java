package com.taxonic.carml.rdf_mapper.impl;

import java.util.List;
import java.util.Optional;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;

class SinglePropertyValueMapper implements PropertyValueMapper {

	private IRI predicate;
	private ValueTransformer valueTransformer;
	
	public SinglePropertyValueMapper(IRI predicate, ValueTransformer valueTransformer) {
		this.predicate = predicate;
		this.valueTransformer = valueTransformer;
	}

	@Override
	public Optional<Object> map(Model model, Object instance, List<Value> values) {
		
		// multiple values present - error
		if (values.size() > 1) {
			throw new RuntimeException("multiple values for property [" + predicate + "], but "
				+ "corresponding java property is NOT an Iterable property");
		}

		if (!values.isEmpty()) {
			Object result = valueTransformer.transform(model, values.get(0));
			return Optional.of(result);
		}
		
		return Optional.empty();
	}

}
