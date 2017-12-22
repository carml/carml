package com.taxonic.carml.model.impl;

import org.eclipse.rdf4j.model.Value;

import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;

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

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (getClass() != obj.getClass()) return false;
		return true;
	}
	
	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder
		extends CarmlTermMap.Builder {
		
		public Builder reference(String reference) {
			super.reference(reference);
			return this;
		}
		
		public Builder inverseExpression(String inverseExpression) {
			super.inverseExpression(inverseExpression);
			return this;
		}
		
		public Builder template(String template) {
			super.template(template);
			return this;
		}
		
		public Builder termType(TermType termType) {
			super.termType(termType);
			return this;
		}
		
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

