package com.taxonic.rml.engine;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Test;

import com.taxonic.rml.engine.template.TemplateParser;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.util.IoUtils;
import com.taxonic.rml.util.RmlMappingLoader;

// TODO consider dynamically creating tests using junit and just iterating over sets of input files
// TODO create unit tests with more than one graphMaps w/ several subject classes.
public class RmlMapperTest {

	private RmlMappingLoader loader = RmlMappingLoader.build();

	@Test
	public void testGraphMapMappingMultipleGraphsB() {
		testMapping("RmlMapper/test13/graphMapMappingMultipleGraphsB.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleGraphsB.output.trig",
				"RmlMapper");
	}
	
	@Test
	public void testGraphMapMappingMultipleGraphsA() {
		testMapping("RmlMapper/test13/graphMapMappingMultipleGraphsA.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleGraphsA.output.trig",
				"RmlMapper");
	}
	
	@Test
	public void testGraphMapMappingMultipleClasses() {
		testMapping("RmlMapper/test13/graphMapMappingMultipleClasses.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleClasses.output.trig",
				"RmlMapper");
	}
	
	@Test
	public void testGraphMapMappingSubjectB() {
		testMapping("RmlMapper/test13/graphMapMappingSubjectB.rml.ttl",
				"RmlMapper/test13/graphMapMappingSubjectB.output.trig",
				"RmlMapper");
	}
	
	@Test
	public void testGraphMapMappingPredObj() {
		testMapping("RmlMapper/test13/graphMapMappingPredObj.rml.ttl",
				"RmlMapper/test13/graphMapMappingPredObj.output.trig",
				"RmlMapper");
	}
	
	@Test
	public void testGraphMapMappingSubjectA() {
		testMapping("RmlMapper/test13/graphMapMappingSubjectA.rml.ttl",
				"RmlMapper/test13/graphMapMappingSubjectA.output.trig",
				"RmlMapper");
	}
	
	@Test
	public void testRemoveNonLatinCharsFunction() {
		//TODO Parse fno.ttl
		//TODO include FNMLFunctions.java too
		String functionPath = "RmlMapper/test12/removeNonLatinCharsFunction.fno.ttl";
		testMapping("RmlMapper/test12/removeNonLatinCharsMapping.fnml.ttl",
				"RmlMapper/test12/removeNonLatinCharsMapping.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testToBoolFunction() {
		//TODO Parse fno.ttl
		//TODO include FNMLFunctions.java too
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
		//TODO Note: algorithm for generating blank node ids will change
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
		printModel(result);
		Model expected = IoUtils.parse(outputPath, determineRdfFormat(outputPath));
		assertEquals(expected, result);
	}
	
	private RDFFormat determineRdfFormat(String path) {
		int period = path.lastIndexOf(".");
		if (period == -1)
			return RDFFormat.TURTLE;
		String extension = path.substring(period + 1).toLowerCase();
		if (extension.equals("ttl"))
			return RDFFormat.TURTLE;
		if (extension.equals("trig"))
			return RDFFormat.TRIG;
		throw new RuntimeException(
			"could not determine rdf format from file extension [" + extension + "]");
	}

	private void printModel(Model model) {
		StringWriter writer = new StringWriter();
		Rio.write(model, writer, RDFFormat.TURTLE);
		System.out.println(writer.toString());
	}

}
