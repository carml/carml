package com.taxonic.rml.engine;

import static org.junit.Assert.*;

import java.util.List;

import org.eclipse.rdf4j.model.Model;
import org.junit.Before;
import org.junit.Test;

import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.util.IoUtils;
import com.taxonic.rml.util.RmlMappingLoader;

// TODO consider dynamically creating tests using junit and just iterating over sets of input files

public class RmlMapperTest {

	private RmlMappingLoader loader = RmlMappingLoader.build();
	
	private RmlMapper mapper;
	
	// create a fresh mapper for every @Test
	@Before
	public void createMapper() {
		mapper = new RmlMapper();
	}
	
	@Test
	public void testConstantSubjectShortcutMapping() {
		testMapping("test1/constantSubjectShortcutMapping");
	}
	
	private void testMapping(String resourceBaseName) {
		List<TriplesMap> mapping = loader.load(resourceBaseName + ".rml.ttl");
		Model result = mapper.map(mapping);
		Model expected = IoUtils.parse(resourceBaseName + ".output.ttl");
		assertEquals(expected, result);
	}

}
