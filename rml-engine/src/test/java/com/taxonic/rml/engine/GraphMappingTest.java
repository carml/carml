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

public class GraphMappingTest {

	private RmlMappingLoader loader = RmlMappingLoader.build();
	
	@Test
	public void testGraphMapMappingMultipleGraphsC() {
		testMapping("RmlMapper/test13/graphMapMappingMultipleGraphsC.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleGraphsC.output.trig",
				"RmlMapper");
	}
	
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
