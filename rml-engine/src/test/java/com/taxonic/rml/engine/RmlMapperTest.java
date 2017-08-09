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
	
	public void testGraphMapMappingSubjectB() {
		//TODO Create output 
		testMapping("RmlMapper/test13/graphMapMappingSubjectB.rml.ttl",
				"RmlMapper/test13/graphMapMappingSubjectB.output.ttl",
				"RmlMapper");
	}
	
	public void testGraphMapMappingPredObj() {
		//TODO Create output 
		testMapping("RmlMapper/test13/graphMapMappingPredObj.rml.ttl",
				"RmlMapper/test13/graphMapMappingPredObj.output.ttl",
				"RmlMapper");
	}
	public void testGraphMapMappingSubjectA() {
		//TODO Create output 
		testMapping("RmlMapper/test13/graphMapMappingSubjectA.rml.ttl",
				"RmlMapper/test13/graphMapMappingSubjectA.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testRemoveNonLatinCharsFunction() {
		String functionPath = "RmlMapper/test12/removeNonLatinCharsFunction.fn.ttl";
		testMapping("RmlMapper/test12/removeNonLatinCharsMapping.fnml.ttl",
				"RmlMapper/test12/removeNonLatinCharsMapping.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testToBoolFunction() {
		String functionPath = "RmlMapper/test11/toBoolFunction.fnml.ttl";
		testMapping("RmlMapper/test11/toBoolMapping.fnml.ttl",
				"RmlMapper/test11/toBoolMapping.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testSeparateMapsMapping() {
		testMapping("RmlMapper/test10/separateMapsMapping.rml.ttl",
				"RmlMapper/test10/separateMapsMapping.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testParentTriplesMapping() {
		testMapping("RmlMapper/test9/parentTriplesMapping.rml.ttl",
				"RmlMapper/test9/parentTriplesMapping.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testSimpleBlankNodeMapping() {
		testMapping("RmlMapper/test8/simpleBlankNodeMapping.rml.ttl",
				"RmlMapper/test8/simpleBlankNodeMapping.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testSimple2ConstantMapping() {
		testMapping("RmlMapper/test7/simple2ConstantMapping.rml.ttl",
				"RmlMapper/test7/simple2ConstantMapping.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testSimpleConstantMapping() {
		testMapping("RmlMapper/test6/simpleConstantMapping.rml.ttl",
				"RmlMapper/test6/simpleConstantMapping.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testDataReferenceMapping() {
		testMapping("RmlMapper/test5/dataReferenceMapping.rml.ttl",
					"RmlMapper/test5/dataReferenceMapping.output.ttl",
					"RmlMapper");
	}
	
	@Test
	public void testSimpleTemplateMapping() {
		testMapping("RmlMapper/test4/simpleTemplateMapping.rml.ttl",
					"RmlMapper/test4/simpleTemplateMapping.output.ttl",
					"RmlMapper");
	}
	
	@Test
	public void testSimpleReferenceMapping() {
		testMapping("RmlMapper/test3/simpleReferenceMapping.rml.ttl",
					"RmlMapper/test3/simpleReferenceMapping.output.ttl",
					"RmlMapper");
	}
	
	@Test
	public void testSubjectMapping() {
		testMapping("RmlMapper/test2/subjectMapping.rml.ttl", 
					"RmlMapper/test2/subjectMapping.output.ttl",
					"RmlMapper");
	}
	
	private void testMapping(String rmlPath, String outputPath, String contextPath) {
		List<TriplesMap> mapping = loader.load(rmlPath);
		Function<String, InputStream> sourceResolver =
			s -> RmlMapperTest.class.getClassLoader().getResourceAsStream(contextPath + "/" + s);
		RmlMapper mapper = new RmlMapper(sourceResolver, TemplateParser.build());
		Model result = mapper.map(mapping);
		Model expected = IoUtils.parse(outputPath);
		assertEquals(expected, result);
	}
	

}
