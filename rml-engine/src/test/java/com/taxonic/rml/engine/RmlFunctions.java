package com.taxonic.rml.engine;

import com.taxonic.rml.engine.function.FnoFunction;
import com.taxonic.rml.engine.function.FnoParam;

public class RmlFunctions {

	private static class Ex {
		
		final static String
		
			prefix = "http://example.com/",
			
			toBoolFunction = prefix + "toBoolFunction",
			
			startString = prefix + "startString";
		
	}
	
	@FnoFunction(Ex.toBoolFunction)
	public boolean toBoolFunction(
		@FnoParam(Ex.startString) String startString
	) {	
		if (startString.toLowerCase().equals("yes")) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public String removeNonLatinCharsFunction(String inputString) {
		return inputString.replaceAll("[^A-Za-z0-9]", "");
	}
	
	public String toLowercase(String inputString) {
		return inputString.toLowerCase();
	}
	
}
