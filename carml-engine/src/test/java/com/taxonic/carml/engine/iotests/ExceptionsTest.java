package com.taxonic.carml.engine.iotests;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.taxonic.carml.model.TermType;
import org.junit.Test;

public class ExceptionsTest extends MappingTest {

	@Test
	public void testGeneratorMappingException() {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionGeneratorMappping.rml.ttl", null));
		assertThat(exception.getMessage(), startsWith("could not create generator for map"));
	}

	@Test
	public void testUnknownTermTypeException() {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionUnknownTermTypeMapping.rml.ttl", null));
		assertThat(exception.getMessage(), startsWith(
				String.format(
						"cannot create an instance of enum type [%s]",
						TermType.class.getCanonicalName()
				)
		));
	}

	@Test
	public void testConstantValueException() {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionConstantValueMapping.rml.ttl", null));
		assertThat(exception.getMessage(), startsWith("encountered constant value of type"));
	}

	@Test
	public void testEqualLogicalSourceException() {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionEqualLogicalSourceMapping.rml.ttl", null));
		assertThat(exception.getMessage(), startsWith("Logical sources are not equal."));
	}

	@Test
	public void testTermTypeExceptionA() {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingA.rml.ttl", null));
		assertThat(exception.getMessage(), startsWith("encountered disallowed term type"));
	}

	@Test
	public void testTermTypeExceptionB() {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingB.rml.ttl", null));
		assertThat(exception.getMessage(), startsWith("encountered disallowed term type"));
	}

	@Test
	public void testTermTypeExceptionC() {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingC.rml.ttl", null));
		assertThat(exception.getMessage(), startsWith("encountered disallowed term type"));
	}

	@Test
	public void testTermTypeExceptionD() {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingD.rml.ttl", null));
		assertThat(exception.getMessage(), startsWith("encountered disallowed term type"));
	}

	@Test
	public void testTermTypeExceptionE() {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> testMapping("RmlMapper", "RmlMapper/exceptionTests/exceptionTermTypeMappingE.rml.ttl", null));
		assertThat(exception.getMessage(), startsWith("encountered disallowed term type"));
	}

}
