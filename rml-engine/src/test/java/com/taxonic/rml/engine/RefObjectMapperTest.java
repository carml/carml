package com.taxonic.rml.engine;

import org.junit.Test;

public class RefObjectMapperTest extends MappingTest {

	@Test
	public void testMultiJoinConditionsBMultiTriplesMap() {
		//TODO check whether inference is possible with join conditions
		testMapping("RmlMapper",
				"RmlMapper/test17/multipleJoinConditionsMappingB.rml.ttl",
				"RmlMapper/test17/multipleJoinConditionsMappingB.output.ttl");
	}
	
	@Test
	public void testMultiJoinConditionsA() {
		testMapping("RmlMapper",
				"RmlMapper/test17/multipleJoinConditionsMappingA.rml.ttl",
				"RmlMapper/test17/multipleJoinConditionsMappingB.output.ttl");
	}
			
	@Test
	public void testJoinTriplesMappingDDiffSourceAndIteratorWithList() {
		testMapping("RmlMapper",
				"RmlMapper/test15/joinIntegratedMappingD.rml.ttl",
				"RmlMapper/test15/joinIntegratedMappingD.output.ttl");
	}
	
	@Test
	public void testJoinTriplesMappingASimilarIteratorAndDiffSource() {
		testMapping("RmlMapper",
				"RmlMapper/test15/joinIntegratedMappingA.rml.ttl",
				"RmlMapper/test15/joinIntegratedMappingA.output.ttl");
	}
	
	@Test
	public void testJoinTriplesMappingBWithDiffIteratorAndSource() {
		testMapping("RmlMapper",
				"RmlMapper/test15/joinIntegratedMappingB.rml.ttl",
				"RmlMapper/test15/joinIntegratedMappingB.output.ttl");
	}
	
	@Test
	public void testJoinTriplesMappingCWithDiffIteratorAndSimilarSource() {
		testMapping("RmlMapper",
				"RmlMapper/test15/joinIntegratedMappingC.rml.ttl",
				"RmlMapper/test15/joinIntegratedMappingC.output.ttl");
	}
	
	@Test
	public void testParentTriplesMapping() {
		testMapping("RmlMapper",
				"RmlMapper/test9/parentTriplesMapping.rml.ttl",
				"RmlMapper/test9/parentTriplesMapping.output.ttl");
	}
	


}

