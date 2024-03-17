package io.carml.engine.iotests;

import org.junit.jupiter.api.Test;

class GraphMappingTest extends MappingTester {

    @Test
    void testGraphMapMappingMultipleGraphsC() {
        testMapping(
                "RmlMapper",
                "/RmlMapper/test13/graphMapMappingMultipleGraphsC.rml.ttl",
                "/RmlMapper/test13/graphMapMappingMultipleGraphsC.output.trig");
    }

    @Test
    void testGraphMapMappingMultipleGraphsB() {
        testMapping(
                "RmlMapper",
                "/RmlMapper/test13/graphMapMappingMultipleGraphsB.rml.ttl",
                "/RmlMapper/test13/graphMapMappingMultipleGraphsB.output.trig");
    }

    @Test
    void testGraphMapMappingMultipleGraphsA() {
        testMapping(
                "RmlMapper",
                "/RmlMapper/test13/graphMapMappingMultipleGraphsA.rml.ttl",
                "/RmlMapper/test13/graphMapMappingMultipleGraphsA.output.trig");
    }

    @Test
    void testGraphMapMappingMultipleClasses() {
        testMapping(
                "RmlMapper",
                "/RmlMapper/test13/graphMapMappingMultipleClasses.rml.ttl",
                "/RmlMapper/test13/graphMapMappingMultipleClasses.output.trig");
    }

    @Test
    void testGraphMapMappingSubjectB() {
        testMapping(
                "RmlMapper",
                "/RmlMapper/test13/graphMapMappingSubjectB.rml.ttl",
                "/RmlMapper/test13/graphMapMappingSubjectB.output.trig");
    }

    @Test
    void testGraphMapMappingPredObj() {
        testMapping(
                "RmlMapper",
                "/RmlMapper/test13/graphMapMappingPredObj.rml.ttl",
                "/RmlMapper/test13/graphMapMappingPredObj.output.trig");
    }

    @Test
    void testGraphMapMappingSubjectA() {
        testMapping(
                "RmlMapper",
                "/RmlMapper/test13/graphMapMappingSubjectA.rml.ttl",
                "/RmlMapper/test13/graphMapMappingSubjectA.output.trig");
    }
}
