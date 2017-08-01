package com.taxonic.rml.rdf_mapper.util;

import java.util.Arrays;
import java.util.function.Function;

public class PropertyUtils {

	/**
	 * @param getterOrSetterName Full name of the getter or setter-method of the property. Example: {@code getName}.
	 * @return
	 */
	public static String getPropertyName(String getterOrSetterName) {
		String prefix = getGetterOrSetterPrefix(getterOrSetterName);
		if (prefix == null)
			// no prefix detected - use method name as-is
			return firstToLowerCase(getterOrSetterName);
		return firstToLowerCase(getterOrSetterName.substring(prefix.length()));
	}
	
	private static String getGetterOrSetterPrefix(String name) {
		return
		Arrays.asList("set", "get", "is").stream()
			.filter(p -> name.startsWith(p))
			.filter(p -> startsWithUppercase(name.substring(p.length())))
			.findFirst().orElse(null);
	}
	
	private static boolean startsWithUppercase(String str) {
		if (str.isEmpty()) return false;
		String first = str.substring(0, 1);
		return first.equals(first.toUpperCase());
	}
	
	private static String firstToLowerCase(String str) {
		return transformFirst(str, String::toLowerCase);
	}

	private static String firstToUpperCase(String str) {
		return transformFirst(str, String::toUpperCase);
	}
	
	private static String transformFirst(String str, Function<String, String> f) {
		if (str.isEmpty()) return str;
		String first = str.substring(0, 1);
		return f.apply(first) + str.substring(1);
	}
	
	public static String createSetterName(String property) {
		return "set" + firstToUpperCase(property);
	}
	
}
