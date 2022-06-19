package io.carml.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RdfValuesTest {

  @ParameterizedTest
  @CsvSource({"http://foo.bar, true", "https://foo.bar, true", "urn:isbn:foo-bar, true", "http//foo.bar, false",
      "http://foo.\\u200E.bar, false"})
  void givenIriString_whenIsValidIriCalled_thenValidateCorrectly(String iriString, boolean expected) {
    // Given
    // When
    boolean isValid = RdfValues.isValidIri(iriString);

    // Then
    assertThat(isValid, is(expected));
  }

}
