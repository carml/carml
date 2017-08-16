package com.taxonic.rml.engine;

import static org.junit.Assert.*;

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

public class PredObjectMapsTest {
	private RmlMappingLoader loader = RmlMappingLoader.build();

	@Test
	public void testPredObjectMappingGOnePredwithJoinCondition() {
		testMapping("RmlMapper/test16/predicateObjectMappingG.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingG.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testPredObjectMappingFOnePredwithTwoParents() {
		testMapping("RmlMapper/test16/predicateObjectMappingF.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingF.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testPredObjectMappingEOnePredAndOneParent() {
		testMapping("RmlMapper/test16/predicateObjectMappingE.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingE.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testPredObjectMappingDOnePredAndMultiObjList() {
		testMapping("RmlMapper/test16/predicateObjectMappingD.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingD.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testPredObjectMappingCOnePredAndMultiObj() {
		testMapping("RmlMapper/test16/predicateObjectMappingC.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingC.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testPredObjectMappingBMultiPredAndObj() {
		testMapping("RmlMapper/test16/predicateObjectMappingB.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingB.output.ttl",
				"RmlMapper");
	}
	
	@Test
	public void testPredObjectMappingAMultiPredOneObject() {
		testMapping("RmlMapper/test16/predicateObjectMappingA.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingA.output.ttl",
				"RmlMapper");
	}
	
	private void testMapping(String rmlPath, String outputPath, String contextPath) {
		List<TriplesMap> mapping = loader.load(rmlPath);
		Function<String, InputStream> sourceResolver =
			s -> RmlMapperTest.class.getClassLoader().getResourceAsStream(contextPath + "/" + s);
		RmlMapper mapper = new RmlMapper(sourceResolver, TemplateParser.build());
		Model result = mapper.map(mapping);
		System.out.println("\n Generated triples from test: " + rmlPath);
		System.out.println("Result: ");
		printModel(result);
		System.out.println("Expected: ");
		Model expected = IoUtils.parse(outputPath, determineRdfFormat(outputPath));
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
