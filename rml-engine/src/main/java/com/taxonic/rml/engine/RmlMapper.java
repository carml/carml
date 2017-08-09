package com.taxonic.rml.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import com.jayway.jsonpath.JsonPath;
import com.taxonic.rml.engine.template.Template;
import com.taxonic.rml.engine.template.Template.Builder;
import com.taxonic.rml.engine.template.TemplateParser;
import com.taxonic.rml.model.LogicalSource;
import com.taxonic.rml.model.PredicateMap;
import com.taxonic.rml.model.SubjectMap;
import com.taxonic.rml.model.TriplesMap;

public class RmlMapper {

	private Function<String, InputStream> sourceResolver;
	private TemplateParser templateParser;
	
	public RmlMapper(
		Function<String, InputStream> sourceResolver,
		TemplateParser templateParser
	) {
		this.sourceResolver = sourceResolver;
		this.templateParser = templateParser;
	}

	public Model map(List<TriplesMap> mapping) {
		
		return null;
	}
	
	private void map(TriplesMap map) {
		
	}
	
	private void createMapper(TriplesMap map) {
		
	}
	
}
