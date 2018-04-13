package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import org.eclipse.rdf4j.model.Value;

public class CarmlGraphMap extends CarmlTermMap implements GraphMap{

	public CarmlGraphMap() {}

	public CarmlGraphMap(
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
		return "CarmlGraphMap [getReference()=" + getReference() + ", getInverseExpression()="
			+ getInverseExpression() + ", getTemplate()=" + getTemplate() + ", getTermType()=" + getTermType()
			+ ", getConstant()=" + getConstant() + ", getFunctionValue()=" + getFunctionValue() + "]";
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

		public CarmlGraphMap build() {
			return new CarmlGraphMap(
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

