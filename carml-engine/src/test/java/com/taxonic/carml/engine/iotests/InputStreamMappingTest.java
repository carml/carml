package com.taxonic.carml.engine.iotests;

import java.io.InputStream;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class InputStreamMappingTest extends MappingTest {

  @Test
  public void testSimpleReferenceMapping() {

    InputStream inputStream = InputStreamMappingTest.class.getClassLoader()
        .getResourceAsStream("RmlMapper/inputStream/input.json");

    testMapping("RmlMapper", "RmlMapper/inputStream/mapping.ttl", "RmlMapper/inputStream/output.ttl", m -> {
    }, Map.of("input", inputStream));
  }

  @Test
  public void given_mappingWithDoublyDefinedEqualLogicalSource_ShouldBeTreatedEqual() {
    InputStream inputStream = InputStreamMappingTest.class.getClassLoader()
        .getResourceAsStream("RmlMapper/simpleTestInput.json");

    testMapping("RmlMapper", "RmlMapper/multDefLogicalSource/multiplyDefinedEqualLogicalSource.rml.ttl",
        "RmlMapper/multDefLogicalSource/output.ttl", m -> {
        }, Map.of("stream-A", inputStream));
  }

}
