package com.taxonic.rml.engine;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.util.IoUtils;
import com.taxonic.rml.util.RmlMappingLoader;

class MappingTest {

	private RmlMappingLoader loader = RmlMappingLoader.build();

	void testMapping(String contextPath, String rmlPath) {
		testMapping(contextPath, rmlPath, null);
	}
	
	void testMapping(String contextPath, String rmlPath, String outputPath) {
		List<TriplesMap> mapping = loader.load(rmlPath);
		Function<String, InputStream> sourceResolver =
			s -> RmlMapperTest.class.getClassLoader().getResourceAsStream(contextPath + "/" + s);
		RmlMapper mapper = new RmlMapper(sourceResolver);
		Model result = mapper.map(mapping);
		
		System.out.println("Generated from test: " + rmlPath);
		System.out.println("This is result: ");
		printModel(result);
		
		// exit for tests without expected output, such as exception tests
		if (outputPath == null) return;
		
		Model expected = IoUtils.parse(outputPath, determineRdfFormat(outputPath));
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
