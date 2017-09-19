package com.taxonic.carml.engine.template;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableInt;

class TemplateImpl implements Template {

	abstract static class Segment {
		
		private String value;

		Segment(String value) {
			this.value = value;
		}

		public String getValue() {
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
	
	static class ExpressionSegment extends Segment {

		private int id;

		ExpressionSegment(int id, String value) {
			super(value);
			this.id = id;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			ExpressionSegment other = (ExpressionSegment) obj;
			if (id != other.id) return false;
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
			result = prime * result + id;
			result = prime * result + ((getValue() == null) ? 0 : getValue().hashCode());
			return result;
		}
		
		@Override
		public String toString() {
			return "ExpressionSegment [getValue()=" + getValue() + "]";
		}	
	}

	private static class ExpressionImpl implements Expression {

		private int id;
		String value;

		ExpressionImpl(int id, String value) {
			this.id = id;
			this.value = value;
		}

		@Override
		public String getValue() {
			return value;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + id;
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			ExpressionImpl other = (ExpressionImpl) obj;
			if (id != other.id) return false;
			if (value == null) {
				if (other.value != null) return false;
			}
			else if (!value.equals(other.value)) return false;
			return true;
		}

		@Override
		public String toString() {
			return "ExpressionImpl [id=" + id + ", value=" + value + "]";
		}
	}
	
	private class Builder implements Template.Builder {

		private Map<Expression, Function<Expression, Optional<String>>> bindings = new LinkedHashMap<>();
		
		@Override
		public Template.Builder bind(Expression expression, Function<Expression, Optional<String>> templateValue) {
			bindings.put(expression, templateValue);
			return this;
		}
		
		private Optional<String> getExpressionValue(Expression expression) {
			if (!bindings.containsKey(expression))
				throw new RuntimeException("no binding present for expression [" + expression + "]");
			return bindings.get(expression).apply(expression);
		}
		
		private Optional<String> getExpressionSegmentValue(ExpressionSegment segment) {
			Expression expression = expressionSegmentMap.get(segment);
			if (expression == null)
				throw new RuntimeException("no Expression instance present corresponding to segment " + segment); // (should never occur)
			return getExpressionValue(expression);
		}
		
		private void checkBindings() {
			if (!new LinkedHashSet<>(bindings.keySet()).equals(expressions))
				throw new RuntimeException("set of bindings [" + bindings.keySet() +
					"] does NOT match set of expressions in template [" + expressions + "]");
		}
		
		@Override
		public Optional<Object> create() {
			checkBindings();
			StringBuilder str = new StringBuilder();
			
			for (Segment s : segments) {
				if (s instanceof Text) {
					str.append(s.getValue());
				} else if (s instanceof ExpressionSegment) {
					Optional<String> value = getExpressionSegmentValue((ExpressionSegment) s);
					if (value.isPresent()) {
						str.append(value.get());
					} else {
						// Return null when an expression value is null.
						// see https://www.w3.org/TR/r2rml/#from-template
						// and https://www.w3.org/TR/r2rml/#generated-rdf-term
						// TODO: PM: is this the best place to check this? Could possibly be handled at builder.
						return Optional.empty();
					}
				}
			}			
			return Optional.of(str.toString());
		}
	}
	
	private static Map<ExpressionSegment, Expression> createExpressionSegmentMap(List<Segment> segments) {
		MutableInt id = new MutableInt();
		return segments.stream()
			.filter(s -> s instanceof ExpressionSegment)
			.map(s -> (ExpressionSegment) s)
			.collect(Collectors.toMap(
				e -> e,
				e -> new ExpressionImpl(id.getAndIncrement(), e.getValue())
			));
	}
	
	static TemplateImpl build(List<Segment> segments) {
		Map<ExpressionSegment, Expression> expressionSegmentMap = createExpressionSegmentMap(segments);
		Set<Expression> expressions = new LinkedHashSet<>(expressionSegmentMap.values());
		return new TemplateImpl(
			segments,
			expressions,
			expressionSegmentMap
		);
	}
	
	private List<Segment> segments;
	private Set<Expression> expressions;
	private Map<ExpressionSegment, Expression> expressionSegmentMap;

	TemplateImpl(
		List<Segment> segments,
		Set<Expression> expressions,
		Map<ExpressionSegment, Expression> expressionSegmentMap
	) {
		this.segments = segments;
		this.expressions = expressions;
		this.expressionSegmentMap = expressionSegmentMap;
	}
	
	@Override
	public Set<Expression> getExpressions() {
		return expressions;
	}

	@Override
	public Template.Builder newBuilder() {
		return new Builder();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((expressionSegmentMap == null) ? 0 : expressionSegmentMap.hashCode());
		result = prime * result + ((expressions == null) ? 0 : expressions.hashCode());
		result = prime * result + ((segments == null) ? 0 : segments.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		TemplateImpl other = (TemplateImpl) obj;
		if (expressionSegmentMap == null) {
			if (other.expressionSegmentMap != null) return false;
		}
		else if (!expressionSegmentMap.equals(other.expressionSegmentMap)) return false;
		if (expressions == null) {
			if (other.expressions != null) return false;
		}
		else if (!expressions.equals(other.expressions)) return false;
		if (segments == null) {
			if (other.segments != null) return false;
		}
		else if (!segments.equals(other.segments)) return false;
		return true;
	}

	@Override
	public String toString() {
		return "TemplateImpl [segments=" + segments + ", expressions=" + expressions + ", expressionSegmentMap="
			+ expressionSegmentMap + "]";
	}

}
