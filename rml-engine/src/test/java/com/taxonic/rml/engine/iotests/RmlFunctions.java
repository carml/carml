package com.taxonic.rml.engine.iotests;

import com.taxonic.rml.engine.function.FnoFunction;
import com.taxonic.rml.engine.function.FnoParam;

public class RmlFunctions {

	private static class Ex {
		
		final static String
		
			prefix = "http://example.com/",
			
			toBoolFunction = prefix + "toBoolFunction",
			
			startString = prefix + "startString",
			
			stringParam = prefix + "stringParam",
			
			removeNonLatinCharsFunction = prefix + "removeNonLatinCharsFunction",
			
			sumFunction = prefix + "sumFunction",
			
			toIntFunction = prefix + "toIntFunction",
			
			toIntOutput = prefix + "toIntOutput",
			
			intParam = prefix + "intParam";
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
	
	@FnoFunction(Ex.toIntFunction)
	public int toIntFunction(
		@FnoParam(Ex.stringParam) String inputString
	) {
		return Integer.parseInt(inputString);
	}
	
	@FnoFunction(Ex.sumFunction)
	public int sumFunction(
		//TODO This probably should be @FnoOutput
		@FnoParam(Ex.toIntOutput) int toIntOutput, @FnoParam(Ex.intParam) int inputInt
	) {
		return toIntOutput + inputInt;
	}
}
