package resources.functions;

public class FNMLFunctions {

	public static boolean toBoolFunction(String startString) {
		if (startString.toLowerCase() == "yes") {
			return true;
		} else {
			return false;
		}
	}

	public static String removeNonLatinCharsFunction(String inputString) {
		return inputString.replaceAll("[^A-Za-z0-9]", "");
	}

	public static String toLowercase(String inputString) {
		return inputString.toLowerCase();
	}
}
