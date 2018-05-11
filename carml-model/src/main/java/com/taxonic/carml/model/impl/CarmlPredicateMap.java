package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.Value;

public class CarmlPredicateMap extends CarmlTermMap implements PredicateMap {

	public CarmlPredicateMap() {}

	public CarmlPredicateMap(
		String reference,
		String inverseExpression,
		String template,
		TermType termType,
		Value constant,
		TriplesMap functionValue
	) {
		super(reference, inverseExpression, template, termType, constant, functionValue);
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder
		extends CarmlTermMap.Builder {

		@Override
		public Builder reference(String reference) {
			super.reference(reference);
			return this;
		}

		@Override
		public Builder inverseExpression(String inverseExpression) {
			super.inverseExpression(inverseExpression);
			return this;
		}

		@Override
		public Builder template(String template) {
			super.template(template);
			return this;
		}

		@Override
		public Builder termType(TermType termType) {
			super.termType(termType);
			return this;
		}

		@Override
		public Builder constant(Value constant) {
			super.constant(constant);
			return this;
		}

		@Override
		public Builder functionValue(TriplesMap functionValue) {
			super.functionValue(functionValue);
			return this;
		}

		public CarmlPredicateMap build() {
			return new CarmlPredicateMap(
				getReference(),
				getInverseExpression(),
				getTemplate(),
				getTermType(),
				getConstant(),
				getFunctionValue()
			);
		}
	}
}

