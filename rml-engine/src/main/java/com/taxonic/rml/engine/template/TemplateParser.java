package com.taxonic.rml.engine.template;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;

public class TemplateParser {

	private String escapableChars;
	
	private TemplateParser(
		String escapableChars
	) {
		this.escapableChars = escapableChars;
	}

	public static TemplateParser build() {
		return new TemplateParser("{}\\");
	}
	
	private abstract static class Segment {
		
		StringBuilder str = new StringBuilder();

		void append(String c) {
			str.append(c);
		}
		
		String value() {
			return str.toString();
		}
	}
	
	private static class Text extends Segment {}
	
	private static class Variable extends Segment {}
	
	public Template parse(String template) {

		MutableBoolean escaping = new MutableBoolean(false);
		MutableObject<Segment> segmentContainer = new MutableObject<Segment>(new Text());
		
		List<TemplateImpl.Segment> segments = new ArrayList<>();
		
		Runnable close = () -> {
			Segment segment = segmentContainer.getValue();
			if (segment == null) return;
			String value = segment.value();
			if (value.isEmpty()) return;
			if (segment instanceof Text)
				segments.add(new TemplateImpl.Text(value));
			else if (segment instanceof Variable)
				segments.add(new TemplateImpl.Variable(value));
			// (assuming no other segment types)
		};
		
		IntStream.range(0, template.length())
			.mapToObj(i -> template.substring(i, i + 1))
			.forEach(c -> {
			
				Segment segment = segmentContainer.getValue();
				
				if (escaping.booleanValue()) {
					if (!escapableChars.contains(c))
						throw new RuntimeException("invalid escape sequence in template [" + template + "] - escaping char [" + c + "]");
					segment.append(c);
					escaping.setFalse();
				}
				
				else if (c.equals("\\"))
					escaping.setTrue();
				
				else if (c.equals("{")) {
					if (segment instanceof Variable)
						throw new RuntimeException("encountered unescaped nested { character in template [" + template + "]");
					// (assuming segment is Text)
					close.run();
					segmentContainer.setValue(new Variable());
				}
				
				else if (c.equals("}")) {
					if (segment instanceof Variable) {
						close.run();
						segmentContainer.setValue(new Text());
					}
					// allow adding } outside variables to text segment
					else segment.append(c);
					// (assuming no other segment types)
				}
				
				else segment.append(c);
			
			});
		
		Segment segment = segmentContainer.getValue();
		if (segment instanceof Variable)
			throw new RuntimeException("unclosed variable expression in template [" + template + "]");
		close.run();
		
		return TemplateImpl.build(segments);
	}
	
}
