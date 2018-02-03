package com.taxonic.carml.engine.iotests;

import com.google.common.collect.ImmutableList;
import com.taxonic.carml.engine.function.FnoFunction;
import com.taxonic.carml.engine.function.FnoParam;
import java.util.List;

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
			
			intParam = prefix + "intParam",
			
			constantListFunction = prefix + "constantListFunction";
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
		@FnoParam(Ex.toIntOutput) int toIntOutput,
		@FnoParam(Ex.intParam) int inputInt
	) {
		return toIntOutput + inputInt;
	}
	
	@FnoFunction(Ex.constantListFunction)
	public List<String> contantListFunction() {
		return 
				ImmutableList.of(
					Ex.prefix + "abc",
					Ex.prefix + "def",
					Ex.prefix + "ghi"
				);
	}
	
	//TODO: PM: Add test for when parameter is not found
	//TODO: PM: Add test for when function returns null

}
