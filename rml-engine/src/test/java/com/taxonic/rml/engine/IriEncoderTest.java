package com.taxonic.rml.engine;

import static org.junit.Assert.assertEquals;

import java.util.function.Function;

import org.junit.Test;

public class IriEncoderTest {

	private Function<String, String> encoder = IriEncoder.create();

	// TODO could use a lot more tests
	
	@Test
	public void variousTests() {
		test("hello there", "hello%20there");
		test("[test]", "%5btest%5d");
	}
	
	private void test(String toEncode, String expectedResult) {
		String encoded = encode(toEncode);
//		System.out.println(toEncode + " ==> " + encoded);
		assertEquals(expectedResult, encoded);
	}
	
	private String encode(String input) {
		return encoder.apply(input);
	}
	
}
