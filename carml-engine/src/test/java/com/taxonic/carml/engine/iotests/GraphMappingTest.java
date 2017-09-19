package com.taxonic.carml.engine.iotests;

import org.junit.Test;

public class GraphMappingTest extends MappingTest {

	@Test
	public void testGraphMapMappingMultipleGraphsC() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingMultipleGraphsC.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleGraphsC.output.trig");
	}
	
	@Test
	public void testGraphMapMappingMultipleGraphsB() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingMultipleGraphsB.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleGraphsB.output.trig");
	}
	
	@Test
	public void testGraphMapMappingMultipleGraphsA() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingMultipleGraphsA.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleGraphsA.output.trig");
	}
	
	@Test
	public void testGraphMapMappingMultipleClasses() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingMultipleClasses.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleClasses.output.trig");
	}
	
	@Test
	public void testGraphMapMappingSubjectB() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingSubjectB.rml.ttl",
				"RmlMapper/test13/graphMapMappingSubjectB.output.trig");
	}
	
	@Test
	public void testGraphMapMappingPredObj() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingPredObj.rml.ttl",
				"RmlMapper/test13/graphMapMappingPredObj.output.trig");
	}
	
	@Test
	public void testGraphMapMappingSubjectA() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingSubjectA.rml.ttl",
				"RmlMapper/test13/graphMapMappingSubjectA.output.trig");
	}
	
}
