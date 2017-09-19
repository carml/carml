package com.taxonic.carml.util;

import static org.junit.Assert.assertEquals;

import java.util.function.Function;

import org.junit.Test;

import com.taxonic.carml.util.IriEncoder;

public class IriEncoderTest {

	private Function<String, String> encoder = IriEncoder.create();

	@Test
	public void encoder_givenStringWithSpace_encodesAsExpected() {
		test("hello there", "hello%20there");
	}
	
	@Test
	public void encoder_givenNotToBeEncodedString_DoesNothing() {
		test("test-tester", "test-tester");
		test("test_tester", "test_tester");
		test("test.tester", "test.tester");
		test("~test", "~test");
		test("葉篤正", "葉篤正");
	}
	
	@Test
	public void encoder_givenParentheses_encodesAsExpected() {
		test("(test)", "%28test%29");
	}
	
	@Test
	public void encoder_givenUri_encodesAsExpected() {
		test("http://example.com","http%3a%2f%2fexample.com");
	}
	
	@Test
	public void encoder_givenEncodingToken_encodesAsExpected() {
		test("100%", "100%25");
		test("1,2", "1%2c2");
	}

	private void test(String toEncode, String expectedResult) {
		String encoded = encode(toEncode);
		System.out.println(toEncode + " ==> " + encoded);
		assertEquals(expectedResult, encoded);
	}
	
	private String encode(String input) {
		return encoder.apply(input);
	}
	
}
