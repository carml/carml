package com.taxonic.rml.engine.iotests;

import org.junit.Test;

public class RefObjectMapperTest extends MappingTest {

	@Test
	public void testMultiJoinConditionsCWithBlankNode() {
		testMapping("RmlMapper",
				"RmlMapper/test17/multipleJoinConditionsMappingC.rml.ttl",
				"RmlMapper/test17/multipleJoinConditionsMappingC.output.ttl");
	}
	
	@Test
	public void testMultiJoinConditionsBMultiTriplesMap() {
		testMapping("RmlMapper",
				"RmlMapper/test17/multipleJoinConditionsMappingB.rml.ttl",
				"RmlMapper/test17/multipleJoinConditionsMappingB.output.ttl");
	}
	
	@Test
	public void testMultiJoinConditionsA() {
		testMapping("RmlMapper",
				"RmlMapper/test17/multipleJoinConditionsMappingA.rml.ttl",
				"RmlMapper/test17/multipleJoinConditionsMappingA.output.ttl");
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
	
	@Test
	public void testParentTriplesMappingWithList() {
		testMapping("RmlMapper",
				"RmlMapper/test9/parentTriplesMappingList.rml.ttl",
				"RmlMapper/test9/parentTriplesMappingList.output.ttl");
	}
	
	@Test
	public void testParentTriplesMappingWithBlankNodeSubjectTemplate() {
		testMapping("RmlMapper",
				"RmlMapper/test9/parentTriplesMappingBlankNodeA.rml.ttl",
				"RmlMapper/test9/parentTriplesMappingBlankNode.output.ttl");
	}
	
	@Test
	public void testParentTriplesMappingWithBlankNodeSubjectReference() {
		testMapping("RmlMapper",
				"RmlMapper/test9/parentTriplesMappingBlankNodeB.rml.ttl",
				"RmlMapper/test9/parentTriplesMappingBlankNode.output.ttl");
	}
	
	@Test
	public void testParentTriplesMappingWithBlankNodeParentTemplate() {
		testMapping("RmlMapper",
				"RmlMapper/test9/parentTriplesMappingBlankNodeParentA.rml.ttl",
				"RmlMapper/test9/parentTriplesMappingBlankNodeParent.output.ttl");
	}
	
	@Test
	public void testParentTriplesMappingWithBlankNodeParentReference() {
		testMapping("RmlMapper",
				"RmlMapper/test9/parentTriplesMappingBlankNodeParentB.rml.ttl",
				"RmlMapper/test9/parentTriplesMappingBlankNodeParent.output.ttl");
	}


}

