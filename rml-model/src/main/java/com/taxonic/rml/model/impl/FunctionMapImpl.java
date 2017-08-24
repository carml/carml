package com.taxonic.rml.model.impl;

import org.eclipse.rdf4j.model.Value;

import com.taxonic.rml.model.FunctionMap;
import com.taxonic.rml.model.TermType;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.rml.rdf_mapper.annotations.RdfType;
import com.taxonic.rml.vocab.Fnml;

public class FunctionMapImpl extends TermMapImpl implements FunctionMap {

	private TriplesMap functionValue;

	public FunctionMapImpl() {}

	public FunctionMapImpl(
		String reference,
		String inverseExpression,
		String template,
		TermType termType,
		Value constant,
		TriplesMap functionValue
	) {
		super(reference, inverseExpression, template, termType, constant);
		this.functionValue = functionValue;
	}

	@RdfProperty(Fnml.functionValue)
	@RdfType(TriplesMapImpl.class)
	@Override
	public TriplesMap getFunctionValue() {
		return functionValue;
	}

	public void setFunctionValue(TriplesMap functionValue) {
		this.functionValue = functionValue;
	}

	@Override
	public String toString() {
		return "FunctionMapImpl [functionValue=" + functionValue + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((functionValue == null) ? 0 : functionValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (getClass() != obj.getClass()) return false;
		FunctionMapImpl other = (FunctionMapImpl) obj;
		if (functionValue == null) {
			if (other.functionValue != null) return false;
		}
		else if (!functionValue.equals(other.functionValue)) return false;
		return true;
	}
	
	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder
		extends com.taxonic.rml.model.impl.TermMapImpl.Builder {
		
		private TriplesMap functionValue;

		Builder() {}
		
		// TODO the value of extending TermMapImpl.Builder is very small...

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
			this.functionValue = functionValue;
			return this;
		}
		
		public FunctionMapImpl build() {
			return new FunctionMapImpl(
				getReference(),
				getInverseExpression(),
				getTemplate(),
				getTermType(),
				getConstant(),
				functionValue
			);
		}
	}

}
