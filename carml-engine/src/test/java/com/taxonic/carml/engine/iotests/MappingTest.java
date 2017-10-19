package com.taxonic.carml.engine.iotests;

import static org.junit.Assert.assertEquals;

import java.util.Set;
import java.util.function.Consumer;

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
		testMapping(contextPath, rmlPath, outputPath, m -> {}, m -> {});
	}
	
	void testMapping(
		String contextPath,
		String rmlPath,
		String outputPath,
		Consumer<RmlMapper.Builder> configureMapper
	) {
		testMapping(contextPath, rmlPath, outputPath, configureMapper, m -> {});
	}
	
	void testMapping(
		String contextPath,
		String rmlPath,
		String outputPath,
		Consumer<RmlMapper.Builder> configureMapper,
		Consumer<RmlMapper> configureMapperInstance
	) {
		Set<TriplesMap> mapping = loader.load(rmlPath, RDFFormat.TURTLE);
		RmlMapper.Builder builder = RmlMapper.newBuilder()
				.classPathResolver(contextPath);
		configureMapper.accept(builder);
		RmlMapper mapper = builder.build();
		configureMapperInstance.accept(mapper);
		Model result = mapper.map(mapping);
		
		//TODO: PM: Add debug logging
//		System.out.println("Generated from test: " + rmlPath);
//		System.out.println("This is result: ");
//		printModel(result);
		
		// exit for tests without expected output, such as exception tests
		if (outputPath == null) return;
		
		Model expected = IoUtils.parse(outputPath, determineRdfFormat(outputPath));
//		System.out.println("This is expected: ");
//		printModel(expected);
		assertEquals(expected, result);
	}
	
	RDFFormat determineRdfFormat(String path) {
		return 
			Rio.getParserFormatForFileName(path)
			.orElseThrow(() -> 
				new RuntimeException("could not determine rdf format from file [" + path + "]"));
	}

//	private void printModel(Model model) {
//		StringWriter writer = new StringWriter();
//		Rio.write(model, writer, RDFFormat.TURTLE);
//		System.out.println(writer.toString());
//	}
	
}
