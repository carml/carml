package com.taxonic.carml.engine.iotests;

import org.junit.jupiter.api.Test;

class PredObjectMapsTest extends MappingTester {

  @Test
  void testPredObjectMappingGOnePredwithJoinCondition() {
    testMapping("RmlMapper", "/RmlMapper/test16/predicateObjectMappingG.rml.ttl",
        "/RmlMapper/test16/predicateObjectMappingG.output.ttl");
  }

  @Test
  void testPredObjectMappingFOnePredwithTwoParents() {
    testMapping("RmlMapper", "/RmlMapper/test16/predicateObjectMappingF.rml.ttl",
        "/RmlMapper/test16/predicateObjectMappingF.output.ttl");
  }

  @Test
  void testPredObjectMappingEOnePredAndOneParent() {
    testMapping("RmlMapper", "/RmlMapper/test16/predicateObjectMappingE.rml.ttl",
        "/RmlMapper/test16/predicateObjectMappingE.output.ttl");
  }

  @Test
  void testPredObjectMappingCOnePredAndMultiObj() {
    testMapping("RmlMapper", "/RmlMapper/test16/predicateObjectMappingC.rml.ttl",
        "/RmlMapper/test16/predicateObjectMappingC.output.ttl");
  }

  @Test
  void testPredObjectMappingBMultiPredAndObj() {
    testMapping("RmlMapper", "/RmlMapper/test16/predicateObjectMappingB.rml.ttl",
        "/RmlMapper/test16/predicateObjectMappingB.output.ttl");
  }

  @Test
  void testPredObjectMappingAMultiPredOneObject() {
    testMapping("RmlMapper", "/RmlMapper/test16/predicateObjectMappingA.rml.ttl",
        "/RmlMapper/test16/predicateObjectMappingA.output.ttl");
  }

}
