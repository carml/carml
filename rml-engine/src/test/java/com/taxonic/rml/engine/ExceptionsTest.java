package com.taxonic.rml.engine;

import static org.hamcrest.CoreMatchers.startsWith;

import java.io.InputStream;
import java.util.function.Function;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.taxonic.rml.engine.template.TemplateParser;

public class ExceptionsTest extends MappingTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testGeneratorMappingException() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage(startsWith("could not create generator for map"));
		testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionGeneratorMappping.rml.ttl", null);
	}

	@Test
	public void testUnknownTermTypeException() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionUnknownTermTypeMapping.rml.ttl", null);
	}

	@Test
	public void testConstantValueException() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage(startsWith("encountered constant value of type"));
		testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionConstantValueMapping.rml.ttl", null);
	}

	@Test
	public void testEqualLogicalSourceException() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage(startsWith("Logical sources are not equal."));
		testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionEqualLogicalSourceMapping.rml.ttl", null);
	}

	@Test
	public void testReadSourceException() throws RuntimeException {
		Function<String, InputStream> sourceResolver = s -> RmlMapperTest.class.getClassLoader()
				.getResourceAsStream("RmlMapper" + "/" + s);
		RmlMapper tm = new RmlMapper(sourceResolver, TemplateParser.build());
		// TODO change readSource back to private.
		String result = tm.readSource("exceptions.json");
		assert (result.equals("{\"name\":\"carml\"}"));
	}

	@Test
	public void testTermTypeExceptionA() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingA.rml.ttl", null);
	}

	@Test
	public void testTermTypeExceptionB() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingB.rml.ttl", null);
	}

	@Test
	public void testTermTypeExceptionC() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingC.rml.ttl", null);
	}

	@Test
	public void testTermTypeExceptionD() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingD.rml.ttl", null);
	}

	@Test
	public void testTermTypeExceptionE() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingE.rml.ttl", null);
	}

}
