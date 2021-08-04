package com.taxonic.carml.engine.iotests;

import org.junit.jupiter.api.Test;

class RefObjectMapperTest extends MappingTester {

  @Test
  void testDeprecatedMultiJoinConditionsCWithBlankNode() {
    testMapping("RmlMapper", "/RmlMapper/test17/multipleJoinConditionsMappingC.rml.ttl",
        "/RmlMapper/test17/multipleJoinConditionsMappingC.output.ttl");
  }

  @Test
  void testMultiJoinConditionsBMultiTriplesMap() {
    testMapping("RmlMapper", "/RmlMapper/test17/multipleJoinConditionsMappingB.rml.ttl",
        "/RmlMapper/test17/multipleJoinConditionsMappingB.output.ttl");
  }

  @Test
  void testMultiJoinConditionsA() {
    testMapping("RmlMapper", "/RmlMapper/test17/multipleJoinConditionsMappingA.rml.ttl",
        "/RmlMapper/test17/multipleJoinConditionsMappingA.output.ttl");
  }

  @Test
  void testJoinTriplesMappingDDiffSourceAndIteratorWithList() {
    testMapping("RmlMapper", "/RmlMapper/test15/joinIntegratedMappingD.rml.ttl",
        "/RmlMapper/test15/joinIntegratedMappingD.output.ttl");
  }

  @Test
  void testJoinTriplesMappingASimilarIteratorAndDiffSource() {
    testMapping("RmlMapper", "/RmlMapper/test15/joinIntegratedMappingA.rml.ttl",
        "/RmlMapper/test15/joinIntegratedMappingA.output.ttl");
  }

  @Test
  void testJoinTriplesMappingBWithDiffIteratorAndSource() {
    testMapping("RmlMapper", "/RmlMapper/test15/joinIntegratedMappingB.rml.ttl",
        "/RmlMapper/test15/joinIntegratedMappingB.output.ttl");
  }

  @Test
  void testJoinTriplesMappingCWithDiffIteratorAndSimilarSource() {
    testMapping("RmlMapper", "/RmlMapper/test15/joinIntegratedMappingC.rml.ttl",
        "/RmlMapper/test15/joinIntegratedMappingC.output.ttl");
  }

  @Test
  void testJoinTriplesMappingWithMultiObjectMap() {
    testMapping("RmlMapper", "/RmlMapper/test15/joinOnMultiObjectMap.rml.ttl",
        "/RmlMapper/test15/joinOnMultiObjectMap.output.ttl");
  }

  @Test
  void testParentTriplesMapping() {
    testMapping("RmlMapper", "/RmlMapper/test9/parentTriplesMapping.rml.ttl",
        "/RmlMapper/test9/parentTriplesMapping.output.ttl");
  }

  @Test
  void testParentTriplesMappingWithBlankNodeSubjectTemplate() {
    testMapping("RmlMapper", "/RmlMapper/test9/parentTriplesMappingBlankNodeA.rml.ttl",
        "/RmlMapper/test9/parentTriplesMappingBlankNode.output.ttl");
  }

  @Test
  void testParentTriplesMappingWithBlankNodeSubjectReference() {
    testMapping("RmlMapper", "/RmlMapper/test9/parentTriplesMappingBlankNodeB.rml.ttl",
        "/RmlMapper/test9/parentTriplesMappingBlankNode.output.ttl");
  }

  @Test
  void testParentTriplesMappingWithBlankNodeParentTemplate() {
    testMapping("RmlMapper", "/RmlMapper/test9/parentTriplesMappingBlankNodeParentA.rml.ttl",
        "/RmlMapper/test9/parentTriplesMappingBlankNodeParent.output.ttl");
  }


}
