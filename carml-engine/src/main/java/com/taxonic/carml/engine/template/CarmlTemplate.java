package com.taxonic.carml.engine.template;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.ImmutableList;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;

class CarmlTemplate implements Template {

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

		private Map<Expression, Function<Expression, Optional<Object>>> bindings = new LinkedHashMap<>();

		@Override
		public Template.Builder bind(Expression expression, Function<Expression, Optional<Object>> templateValue) {
			bindings.put(expression, templateValue);
			return this;
		}

		private Optional<Object> getExpressionValue(Expression expression) {
			if (!bindings.containsKey(expression))
				throw new RuntimeException("no binding present for expression [" + expression + "]");
			return bindings.get(expression).apply(expression);
		}

		private Optional<Object> getExpressionSegmentValue(ExpressionSegment segment) {
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

		private List<String> getValuesExpressionEvaluation(Object evalResult) {
			if (evalResult instanceof Collection<?>) {
				return
						((Collection<?>) evalResult).stream()
							.map(v -> (String) v)
							.collect(ImmutableCollectors.toImmutableList());
			} else {
				return ImmutableList.of((String)evalResult);
			}
		}

		private boolean exprValueResultsHasOnlyFilledLists(Map<Segment, List<String>> indexedExprValues) {
			for (List<String> list : indexedExprValues.values()) {
				if (list.size() == 0)
					return false;
			}
			return true;
		}

		private int checkExprValueResultsOfEqualSizeAndReturn(Map<Segment, List<String>> indexedExprValues) {
			int size = -1;
			for (List<String> list : indexedExprValues.values()) {
				if (size == -1) {
					size = list.size();
				} else {
					if (list.size() != size) {
						System.out.println(indexedExprValues);
						System.out.println(list);
						throw new RuntimeException(
								String.format("Template expressions do not lead to an equal amount of values: %s",
										indexedExprValues.keySet()));
					}
				}
			}
			return size;
		}

		@Override
		public Optional<Object> create() {
			checkBindings();
			List<String> result = new ArrayList<>();
			Map<Segment, List<String>> indexedExprValues = new HashMap<>();

			List<ExpressionSegment> exprSegs = segments.stream()
				.filter(s -> s instanceof ExpressionSegment)
				.map(s -> (ExpressionSegment) s)
				.collect(Collectors.toList());

			if (exprSegs.size() > 0) {

				for (ExpressionSegment exprSeg: exprSegs) {

					Optional<Object> evalResult = getExpressionSegmentValue(exprSeg);
					indexedExprValues.put(
							exprSeg,
							evalResult
								.map(this::getValuesExpressionEvaluation)
								.orElse(ImmutableList.of())
					);

				}

				if (indexedExprValues.size() > 0) {
					if (!exprValueResultsHasOnlyFilledLists(indexedExprValues)) {
						return Optional.empty();
					}

					int indexedExprValSize = checkExprValueResultsOfEqualSizeAndReturn(indexedExprValues);
					for (int i = 0; i < indexedExprValSize; i++) {
						StringBuilder str = new StringBuilder();
						for (Segment s : segments) {
							if (s instanceof Text) {
								str.append(s.getValue());
							} else if (s instanceof ExpressionSegment) {
								String exprValue = indexedExprValues.get(s).get(i);
								if (exprValue == null) {
									result.add(null);
									continue;
								}
								str.append(exprValue);
							}
						}
						result.add(str.toString());
					}
				}
			} else {
				StringBuilder str = new StringBuilder();
				segments.forEach(s -> str.append(s.getValue()));
				result.add(str.toString());
			}

			return Optional.of(result);
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

	static CarmlTemplate build(List<Segment> segments) {
		Map<ExpressionSegment, Expression> expressionSegmentMap = createExpressionSegmentMap(segments);
		Set<Expression> expressions = new LinkedHashSet<>(expressionSegmentMap.values());
		return new CarmlTemplate(
			segments,
			expressions,
			expressionSegmentMap
		);
	}

	private List<Segment> segments;
	private Set<Expression> expressions;
	private Map<ExpressionSegment, Expression> expressionSegmentMap;

	CarmlTemplate(
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
		CarmlTemplate other = (CarmlTemplate) obj;
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
		return "CarmlTemplate [segments=" + segments + ", expressions=" + expressions + ", expressionSegmentMap="
			+ expressionSegmentMap + "]";
	}

}
