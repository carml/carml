package com.taxonic.rml.engine;

import org.junit.Test;

public class ExceptionsTest extends MappingTest {
	
	@Test(expected = Exception.class)
	public void testReadSourceException() {
		testMapping("RmlMapper",
				"RmlMapper/exceptionTests/exceptionReadSourceMapping.rml.ttl");
	}
	
	@Test(expected = Exception.class)
	public void testTermTypeExceptionA() {
		testMapping("RmlMapper",
				"RmlMapper/exceptionTests/exceptionTermTypeMappingA.rml.ttl");
	}
	
	@Test(expected = Exception.class)
	public void testTermTypeExceptionB() {
		testMapping("RmlMapper",
				"RmlMapper/exceptionTests/exceptionTermTypeMappingB.rml.ttl");
	}
	
	@Test(expected = Exception.class)
	public void testTermTypeExceptionC() {
		testMapping("RmlMapper",
				"RmlMapper/exceptionTests/exceptionTermTypeMappingC.rml.ttl");
	}
	
	@Test(expected = Exception.class)
	public void testTermTypeExceptionD() {
		testMapping("RmlMapper",
				"RmlMapper/exceptionTests/exceptionTermTypeMappingD.rml.ttl");
	}
	
	@Test(expected = Exception.class)
	public void testTermTypeExceptionE() {
		testMapping("RmlMapper",
				"RmlMapper/exceptionTests/exceptionTermTypeMappingE.rml.ttl");
	}

}
