package com.taxonic.rml.engine;

import org.junit.Test;

public class PredObjectMapsTest extends MappingTest {

	@Test
	public void testPredObjectMappingGOnePredwithJoinCondition() {
		testMapping("RmlMapper",
				"RmlMapper/test16/predicateObjectMappingG.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingG.output.ttl");
	}
	
	@Test
	public void testPredObjectMappingFOnePredwithTwoParents() {
		testMapping("RmlMapper",
				"RmlMapper/test16/predicateObjectMappingF.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingF.output.ttl");
	}
	
	@Test
	public void testPredObjectMappingEOnePredAndOneParent() {
		testMapping("RmlMapper",
				"RmlMapper/test16/predicateObjectMappingE.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingE.output.ttl");
	}
	
	@Test
	public void testPredObjectMappingDOnePredAndMultiObjList() {
		testMapping("RmlMapper",
				"RmlMapper/test16/predicateObjectMappingD.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingD.output.ttl");
	}
	
	@Test
	public void testPredObjectMappingCOnePredAndMultiObj() {
		testMapping("RmlMapper",
				"RmlMapper/test16/predicateObjectMappingC.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingC.output.ttl");
	}
	
	@Test
	public void testPredObjectMappingBMultiPredAndObj() {
		testMapping("RmlMapper",
				"RmlMapper/test16/predicateObjectMappingB.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingB.output.ttl");
	}
	
	@Test
	public void testPredObjectMappingAMultiPredOneObject() {
		testMapping("RmlMapper",
				"RmlMapper/test16/predicateObjectMappingA.rml.ttl",
				"RmlMapper/test16/predicateObjectMappingA.output.ttl");
	}

}
