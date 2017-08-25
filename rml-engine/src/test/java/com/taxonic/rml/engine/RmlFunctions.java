package com.taxonic.rml.engine;

import com.taxonic.rml.engine.function.FnoFunction;
import com.taxonic.rml.engine.function.FnoParam;

public class RmlFunctions {

	private static class Ex {
		
		final static String
		
			prefix = "http://example.com/",
			
			toBoolFunction = prefix + "toBoolFunction",
			
			startString = prefix + "startString",
			
			removeNonLatinCharsFunction = prefix + "removeNonLatinCharsFunction";
		
	}
	
	@FnoFunction(Ex.toBoolFunction)
	public boolean toBoolFunction(
		@FnoParam(Ex.startString) String startString
	) {
		return startString.toLowerCase().equals("yes");
	}
	
	@FnoFunction(Ex.removeNonLatinCharsFunction)
	public String removeNonLatinCharsFunction(
		@FnoParam(Ex.startString) String inputString
	) {
		return inputString.replaceAll("[^A-Za-z0-9]", "");
	}
	
	public String toLowercase(String inputString) {
		return inputString.toLowerCase();
	}
	
}
