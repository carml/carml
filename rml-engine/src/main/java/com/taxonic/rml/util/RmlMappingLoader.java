package com.taxonic.rml.util;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.Model;

import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.TriplesMapImpl;
import com.taxonic.rml.rdf_mapper.Mapper;
import com.taxonic.rml.rdf_mapper.impl.MapperImpl;
import com.taxonic.rml.vocab.Rdf;

public class RmlMappingLoader {

	private RmlConstantShorthandExpander shorthandExpander;
	
	public RmlMappingLoader(
		RmlConstantShorthandExpander shorthandExpander
	) {
		this.shorthandExpander = shorthandExpander;
	}

	public List<TriplesMap> load(String resource) {

		Model originalModel = IoUtils.parse(resource);
		
		Model model = shorthandExpander.apply(originalModel);
		
		Mapper mapper = new MapperImpl();
		
		return
		Collections.unmodifiableList(
			model
				.filter(null, Rdf.Rml.logicalSource, null)
				.subjects()
				.stream()
				.<TriplesMap>map(r -> mapper.map(model, r, TriplesMapImpl.class))
				.collect(Collectors.toList())
		);
	}
	
	public static RmlMappingLoader build() {
		return new RmlMappingLoader(
			new RmlConstantShorthandExpander()
		);
	}
}
