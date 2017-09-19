package com.taxonic.carml.engine.iotests;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.IoUtils;
import com.taxonic.carml.util.RmlMappingLoader;

class MappingTest {

	private RmlMappingLoader loader = RmlMappingLoader.build();

	void testMapping(String contextPath, String rmlPath) {
		testMapping(contextPath, rmlPath, null);
	}
	
	void testMapping(
		String contextPath,
		String rmlPath,
		String outputPath
	) {
		testMapping(contextPath, rmlPath, outputPath, m -> {});
	}
	
	void testMapping(
		String contextPath,
		String rmlPath,
		String outputPath,
		Consumer<RmlMapper> configureMapper
	) {
		Set<TriplesMap> mapping = loader.load(rmlPath, RDFFormat.TURTLE);
		Function<String, InputStream> sourceResolver =
			s -> RmlMapperTest.class.getClassLoader().getResourceAsStream(contextPath + "/" + s);
		RmlMapper mapper = new RmlMapper(sourceResolver);
		configureMapper.accept(mapper);
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
	
	RDFFormat determineRdfFormat(String path) {
		return 
			Rio.getParserFormatForFileName(path)
			.orElseThrow(() -> 
				new RuntimeException("could not determine rdf format from file [" + path + "]"));
	}

	private void printModel(Model model) {
		StringWriter writer = new StringWriter();
		Rio.write(model, writer, RDFFormat.TURTLE);
		System.out.println(writer.toString());
	}
	
}
