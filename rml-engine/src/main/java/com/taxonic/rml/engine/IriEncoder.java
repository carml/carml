package com.taxonic.rml.engine;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class IriEncoder implements Function<String, String> {

	static String encode(String input) {
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
	
	static Function<String, String> create() {
		
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

	IriEncoder(List<Range> ranges) {
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
