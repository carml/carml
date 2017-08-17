package com.taxonic.rml.engine;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Test;

import com.taxonic.rml.engine.template.TemplateParser;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.util.IoUtils;
import com.taxonic.rml.util.RmlMappingLoader;

public class RefObjectMapperTest {

	private RmlMappingLoader loader = RmlMappingLoader.build();

	@Test
	public void testMultiJoinConditionsBMultiTriplesMap() {
		//TODO check whether inference is possible with join conditions
		testMapping("RmlMapper/test17/multipleJoinConditionsMappingB.rml.ttl",
				"RmlMapper/test17/multipleJoinConditionsMappingB.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testMultiJoinConditionsA() {
		testMapping("RmlMapper/test17/multipleJoinConditionsMappingA.rml.ttl",
				"RmlMapper/test17/multipleJoinConditionsMappingB.output.ttl",
				"RmlMapper");
	}
			
	@Test
	public void testJoinTriplesMappingDDiffSourceAndIteratorWithList() {
		testMapping("RmlMapper/test15/joinIntegratedMappingD.rml.ttl",
				"RmlMapper/test15/joinIntegratedMappingD.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testJoinTriplesMappingASimilarIteratorAndDiffSource() {
		testMapping("RmlMapper/test15/joinIntegratedMappingA.rml.ttl",
				"RmlMapper/test15/joinIntegratedMappingA.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testJoinTriplesMappingBWithDiffIteratorAndSource() {
		testMapping("RmlMapper/test15/joinIntegratedMappingB.rml.ttl",
				"RmlMapper/test15/joinIntegratedMappingB.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testJoinTriplesMappingCWithDiffIteratorAndSimilarSource() {
		testMapping("RmlMapper/test15/joinIntegratedMappingC.rml.ttl",
				"RmlMapper/test15/joinIntegratedMappingC.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testParentTriplesMapping() {
		testMapping("RmlMapper/test9/parentTriplesMapping.rml.ttl",
				"RmlMapper/test9/parentTriplesMapping.output.ttl",
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
		
		System.out.println("Generated from test: " + rmlPath);
		System.out.println("This is result: ");
		printModel(result);
		System.out.println("This is expected: ");
		printModel(expected);
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

