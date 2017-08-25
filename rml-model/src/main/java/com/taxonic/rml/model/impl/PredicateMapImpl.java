package com.taxonic.rml.model.impl;

import org.eclipse.rdf4j.model.Value;

import com.taxonic.rml.model.PredicateMap;
import com.taxonic.rml.model.TermType;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.ObjectMapImpl.Builder;

public class PredicateMapImpl extends TermMapImpl implements PredicateMap {

	public PredicateMapImpl() {}
	
	public PredicateMapImpl(
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
		return "PredicateMapImpl [getReference()=" + getReference() + ", getInverseExpression()="
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
		extends com.taxonic.rml.model.impl.TermMapImpl.Builder {
		
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
		
		public Builder functionValue(TriplesMap functionValue) {
			super.functionValue(functionValue);
			return this;
		}
		
		public PredicateMapImpl build() {
			return new PredicateMapImpl(
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

