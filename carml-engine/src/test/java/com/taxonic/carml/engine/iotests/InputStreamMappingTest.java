package com.taxonic.carml.engine.iotests;

import org.junit.Test;

public class InputStreamMappingTest extends MappingTest {

	@Test
	public void testSimpleReferenceMapping() {
		testMapping("RmlMapper",
					"RmlMapper/inputStream/mapping.ttl",
					"RmlMapper/inputStream/output.ttl");
	}
	
}
