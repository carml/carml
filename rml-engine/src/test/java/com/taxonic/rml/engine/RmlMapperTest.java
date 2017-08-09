package com.taxonic.rml.engine;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.util.List;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Model;
import org.junit.Test;

import com.taxonic.rml.engine.template.TemplateParser;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.util.IoUtils;
import com.taxonic.rml.util.RmlMappingLoader;

// TODO consider dynamically creating tests using junit and just iterating over sets of input files

public class RmlMapperTest {

	private RmlMappingLoader loader = RmlMappingLoader.build();
	
	@Test
	public void testConstantSubjectShortcutMapping() {
		testMapping("RdfMapper/test1", "constantObjectShortcutMapping");
	}
	
	private void testMapping(String resourcePath, String resourceBaseName) {
		List<TriplesMap> mapping = loader.load(resourcePath + "/" + resourceBaseName + ".rml.ttl");
		Function<String, InputStream> sourceResolver =
			s -> RmlMapperTest.class.getClassLoader().getResourceAsStream(resourcePath + "/" + s);
		RmlMapper mapper = new RmlMapper(sourceResolver, TemplateParser.build());
		Model result = mapper.map(mapping);
		Model expected = IoUtils.parse(resourcePath + "/" + resourceBaseName + ".output.ttl");
		assertEquals(expected, result);
	}

}
