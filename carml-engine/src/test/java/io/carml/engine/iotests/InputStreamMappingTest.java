package io.carml.engine.iotests;

import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class InputStreamMappingTest extends MappingTester {

    @Test
    void testSimpleReferenceMapping() {

        InputStream inputStream = InputStreamMappingTest.class.getResourceAsStream("/RmlMapper/inputStream/input.json");

        testMapping(
                "RmlMapper",
                "/RmlMapper/inputStream/mapping.ttl",
                "/RmlMapper/inputStream/output.ttl",
                m -> {},
                Map.of("input", Objects.requireNonNull(inputStream)));
    }

    @Test
    void given_mappingWithDoublyDefinedEqualLogicalSource_ShouldBeTreatedEqual() {
        InputStream inputStream = InputStreamMappingTest.class.getResourceAsStream("/RmlMapper/simpleTestInput.json");

        testMapping(
                "RmlMapper",
                "/RmlMapper/multDefLogicalSource/multiplyDefinedEqualLogicalSource.rml.ttl",
                "/RmlMapper/multDefLogicalSource/output.ttl",
                m -> {},
                Map.of("stream-A", Objects.requireNonNull(inputStream)));
    }
}
