package com.taxonic.carml.util;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Percent-encodes any characters in a {@code String} that are
 * NOT one of the following:
 * - alphabetic, as per {@code Character::isAlphabetic}
 * - a digit, as per {@code Character::isDigit}
 * - a dash (-), dot (.), underscore (_) or tilde (~)
 * - part of any of the ranges of character codes specified when
 *   creating the {@code IriEncoder} through its constructor {@link #IriEncoder(List)}.
 *   When creating an {@code IriEncoder} through {@link #create}, these ranges
 *   correspond to the {@code ucschar} production rule from RFC-TODO.
 */
public class IriEncoder implements Function<String, String> {

	public static String encode(String input) {
		return create().apply(input);
	}
	
	static class Range {
		
		final int from, to;

		Range(int from, int to) {
			this.from = from;
			this.to = to;
		}
		
		boolean includes(int value) {
			return value >= from && value <= to;
		}
	}
	
	public static IriEncoder create() {
		
		/* percent-encode any char not in the 'iunreserved' production rule:
		   iunreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~" / ucschar
		   ucschar        = %xA0-D7FF / %xF900-FDCF / %xFDF0-FFEF
		                  / %x10000-1FFFD / %x20000-2FFFD / %x30000-3FFFD
		                  / %x40000-4FFFD / %x50000-5FFFD / %x60000-6FFFD
		                  / %x70000-7FFFD / %x80000-8FFFD / %x90000-9FFFD
		                  / %xA0000-AFFFD / %xB0000-BFFFD / %xC0000-CFFFD
		                  / %xD0000-DFFFD / %xE1000-EFFFD			 */

		String rangesStr =
			"%xA0-D7FF / %xF900-FDCF / %xFDF0-FFEF " +
				"/ %x10000-1FFFD / %x20000-2FFFD / %x30000-3FFFD " +
				"/ %x40000-4FFFD / %x50000-5FFFD / %x60000-6FFFD " +
				"/ %x70000-7FFFD / %x80000-8FFFD / %x90000-9FFFD " +
				"/ %xA0000-AFFFD / %xB0000-BFFFD / %xC0000-CFFFD " +
				"/ %xD0000-DFFFD / %xE1000-EFFFD";
		
		List<Range> ranges =
			Arrays.asList(rangesStr.split("/")).stream().map(p -> {
				String[] range = p.trim().substring(2).split("-");
				return new Range(
					Integer.parseInt(range[0], 16),
					Integer.parseInt(range[1], 16)
				);
			})
			.collect(Collectors.toList());
		
		return new IriEncoder(ranges);
	}
	
	private List<Range> ranges;

	public IriEncoder(List<Range> ranges) {
		this.ranges = ranges;
	}

	@Override
	public String apply(String s) {
		StringBuilder result = new StringBuilder();
		
		s.codePoints().flatMap(c -> {
			
			if (
				Character.isAlphabetic(c) ||
				Character.isDigit(c) ||
				c == '-' ||
				c == '.' ||
				c == '_' ||
				c == '~' ||
				ranges.stream().anyMatch(r -> r.includes(c))
			)
				return IntStream.of(c);

			// percent-encode
			return ("%" + Integer.toHexString(c)).codePoints();
		})
		.forEach(c -> result.append((char) c));
		return result.toString();
	}
	
}
