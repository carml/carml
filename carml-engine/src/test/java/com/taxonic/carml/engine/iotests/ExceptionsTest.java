package com.taxonic.carml.engine.iotests;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.taxonic.carml.model.TermType;
import org.junit.jupiter.api.Test;

class ExceptionsTest extends MappingTester {

  @Test
  void testGeneratorMappingException() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> testMapping("RmlMapper", "/RmlMapper/exceptionTests/exceptionGeneratorMappping.rml.ttl", null));
    assertThat(exception.getMessage(), startsWith("Exception occurred while creating object generator for"));
  }

  @Test
  void testUnknownTermTypeException() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> testMapping("RmlMapper", "/RmlMapper/exceptionTests/exceptionUnknownTermTypeMapping.rml.ttl", null));
    assertThat(exception.getMessage(),
        startsWith(String.format("cannot create an instance of enum type [%s]", TermType.class.getCanonicalName())));
  }

  @Test
  void testConstantValueException() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> testMapping("RmlMapper", "/RmlMapper/exceptionTests/exceptionConstantValueMapping.rml.ttl", null));
    assertThat(exception.getMessage(), startsWith("Exception occurred while creating predicate generator for"));
  }

  @Test
  void testEqualLogicalSourceException() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> testMapping("RmlMapper", "/RmlMapper/exceptionTests/exceptionEqualLogicalSourceMapping.rml.ttl", null));
    assertThat(exception.getMessage(), startsWith("Logical sources are not equal."));
  }

  @Test
  void testTermTypeExceptionA() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> testMapping("RmlMapper", "/RmlMapper/exceptionTests/exceptionTermTypeMappingA.rml.ttl", null));
    assertThat(exception.getMessage(), startsWith("Exception occurred while creating subject generator for"));
  }

  @Test
  void testTermTypeExceptionB() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> testMapping("RmlMapper", "/RmlMapper/exceptionTests/exceptionTermTypeMappingB.rml.ttl", null));
    assertThat(exception.getMessage(), startsWith("encountered disallowed term type"));
  }

  @Test
  void testTermTypeExceptionC() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> testMapping("RmlMapper", "/RmlMapper/exceptionTests/exceptionTermTypeMappingC.rml.ttl", null));
    assertThat(exception.getMessage(), startsWith("encountered disallowed term type"));
  }

  @Test
  void testTermTypeExceptionD() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> testMapping("RmlMapper", "/RmlMapper/exceptionTests/exceptionTermTypeMappingD.rml.ttl", null));
    assertThat(exception.getMessage(), startsWith("Exception occurred while creating predicate generator for"));
  }

  @Test
  void testTermTypeExceptionE() {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> testMapping("RmlMapper", "/RmlMapper/exceptionTests/exceptionTermTypeMappingE.rml.ttl", null));
    assertThat(exception.getMessage(), startsWith("Exception occurred while creating predicate generator for"));
  }

}
