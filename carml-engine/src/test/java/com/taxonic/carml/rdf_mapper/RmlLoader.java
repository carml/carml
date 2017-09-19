package com.taxonic.carml.rdf_mapper;

import java.util.List;

import org.eclipse.rdf4j.rio.RDFFormat;

import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.RmlMappingLoader;

class RmlLoader {

	private RmlMappingLoader loader = RmlMappingLoader.build();

	protected List<TriplesMap> loadRmlFromTtl(String resource) {
		return loader.load(resource, RDFFormat.TURTLE);
	}

}
