package com.taxonic.rml.engine.template;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class TemplateImpl implements Template {

	abstract static class Segment {
		
		private String value;

		Segment(String value) {
			this.value = value;
		}

		String getValue() {
			return value;
		}
	}
	
	static class Text extends Segment {

		Text(String value) {
			super(value);
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			Text other = (Text) obj;
			if (getValue() == null) {
				if (other.getValue() != null) return false;
			}
			else if (!getValue().equals(other.getValue())) return false;
			return true;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((getValue() == null) ? 0 : getValue().hashCode());
			return result;
		}

		@Override
		public String toString() {
			return "Text [getValue()=" + getValue() + "]";
		}
	}
	
	static class Variable extends Segment {

		Variable(String value) {
			super(value);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			Variable other = (Variable) obj;
			if (getValue() == null) {
				if (other.getValue() != null) return false;
			}
			else if (!getValue().equals(other.getValue())) return false;
			return true;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((getValue() == null) ? 0 : getValue().hashCode());
			return result;
		}
		
		@Override
		public String toString() {
			return "Variable [getValue()=" + getValue() + "]";
		}	
	}
	
	private class Builder implements Template.Builder {

		private Map<String, Object> values = new LinkedHashMap<>();
		
		@Override
		public Template.Builder bind(String variable, Object value) {
			values.put(variable, value);
			return this;
		}
		
		private Object getVariableValue(String variable) {
			if (!values.containsKey(variable))
				throw new RuntimeException("no binding present for variable [" + variable + "]");
			return values.get(variable);
		}
		
		private void checkBindings() {
			if (!new LinkedHashSet<>(values.keySet()).equals(variables))
				throw new RuntimeException("set of bindings [" + values.keySet() +
					"] does NOT match set of variables in template [" + variables + "]");
		}

		@Override
		public String create() {
			checkBindings();
			StringBuilder str = new StringBuilder();
			segments.forEach(s -> {
				if (s instanceof Text)
					str.append(s.getValue());
				else if (s instanceof Variable)
					str.append(getVariableValue(s.getValue()));
			});
			return str.toString();
		}
	}
	
	static TemplateImpl build(List<Segment> segments) {
		Set<String> variables =
			new LinkedHashSet<>(
				segments.stream()
					.filter(s -> s instanceof Variable)
					.map(Segment::getValue)
					.collect(Collectors.toSet())
			);
		return new TemplateImpl(segments, variables);
	}
	
	private List<Segment> segments;
	private Set<String> variables;

	TemplateImpl(List<Segment> segments, Set<String> variables) {
		this.segments = segments;
		this.variables = variables;
	}
	
	@Override
	public Set<String> getVariables() {
		return variables;
	}

	@Override
	public Template.Builder newBuilder() {
		return new Builder();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((segments == null) ? 0 : segments.hashCode());
		result = prime * result + ((variables == null) ? 0 : variables.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		TemplateImpl other = (TemplateImpl) obj;
		if (segments == null) {
			if (other.segments != null) return false;
		}
		else if (!segments.equals(other.segments)) return false;
		if (variables == null) {
			if (other.variables != null) return false;
		}
		else if (!variables.equals(other.variables)) return false;
		return true;
	}

	@Override
	public String toString() {
		return "TemplateImpl [segments=" + segments + "]";
	}
	
}

