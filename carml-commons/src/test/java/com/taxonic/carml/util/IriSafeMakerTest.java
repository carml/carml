package com.taxonic.carml.util;

import static org.junit.Assert.assertEquals;

import java.util.function.Function;
import org.junit.Test;

public class IriSafeMakerTest {

	private Function<String, String> safeMaker = IriSafeMaker.create();

	@Test
	public void safeMaker_givenStringWithSpace_encodesAsExpected() {
		test("hello there", "hello%20there");
	}

	@Test
	public void safeMaker_givenNotToBeEncodedString_DoesNothing() {
		test("test-tester", "test-tester");
		test("test_tester", "test_tester");
		test("test.tester", "test.tester");
		test("~test", "~test");
		test("葉篤正", "葉篤正");
	}

	@Test
	public void safeMaker_givenParentheses_encodesAsExpected() {
		test("(test)", "%28test%29");
	}

	@Test
	public void safeMaker_givenUri_encodesAsExpected() {
		test("http://example.com","http%3a%2f%2fexample.com");
	}

	@Test
	public void safeMaker_givenEncodingToken_encodesAsExpected() {
		test("100%", "100%25");
		test("1,2", "1%2c2");
	}

	private void test(String toMakeIriSafe, String expectedResult) {
		String iriSafeValue = makeSafe(toMakeIriSafe);
		assertEquals(expectedResult, iriSafeValue);
	}

	private String makeSafe(String input) {
		return safeMaker.apply(input);
	}

}
