package com.taxonic.carml.model.impl;

import org.eclipse.rdf4j.model.Value;

import com.taxonic.carml.model.TermMap;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Fnml;
import com.taxonic.carml.vocab.Rml;
import com.taxonic.carml.vocab.Rr;

abstract public class TermMapImpl implements TermMap {

	private String reference;
	private String inverseExpression;
	private String template;
	private TermType termType;
	private Value constant;
	private TriplesMap functionValue;

	public TermMapImpl() {}
	
	public TermMapImpl(
		String reference,
		String inverseExpression,
		String template,
		TermType termType,
		Value constant,
		TriplesMap functionValue
	) {
		this.reference = reference;
		this.inverseExpression = inverseExpression;
		this.template = template;
		this.termType = termType;
		this.constant = constant;
		this.functionValue = functionValue;
	}

	@RdfProperty(Rml.reference)
	@Override
	public String getReference() {
		return reference;
	}

	@RdfProperty(Rr.inverseExpression)
	@Override
	public String getInverseExpression() {
		return inverseExpression;
	}

	@RdfProperty(Rr.template)
	@Override
	public String getTemplate() {
		return template;
	}

	// TODO https://www.w3.org/TR/r2rml/#dfn-term-type
	@RdfProperty(Rr.termType)
	@Override
	public TermType getTermType() {
		return termType;
	}

	@RdfProperty(Rr.constant)
	@Override
	public Value getConstant() {
		return constant;
	}
	
	@RdfProperty(Fnml.functionValue)
	@RdfType(TriplesMapImpl.class)
	@Override
	public TriplesMap getFunctionValue() {
		return functionValue;
	}

	public void setReference(String reference) {
		this.reference = reference;
	}

	public void setInverseExpression(String inverseExpression) {
		this.inverseExpression = inverseExpression;
	}

	public void setTemplate(String template) {
		this.template = template;
	}

	public void setTermType(TermType termType) {
		this.termType = termType;
	}

	public void setConstant(Value constant) {
		this.constant = constant;
	}
	
	public void setFunctionValue(TriplesMap functionValue) {
		this.functionValue = functionValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((constant == null) ? 0 : constant.hashCode());
		result = prime * result + ((functionValue == null) ? 0 : functionValue.hashCode());
		result = prime * result + ((inverseExpression == null) ? 0 : inverseExpression.hashCode());
		result = prime * result + ((reference == null) ? 0 : reference.hashCode());
		result = prime * result + ((template == null) ? 0 : template.hashCode());
		result = prime * result + ((termType == null) ? 0 : termType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		TermMapImpl other = (TermMapImpl) obj;
		if (constant == null) {
			if (other.constant != null) return false;
		}
		else if (!constant.equals(other.constant)) return false;
		if (functionValue == null) {
			if (other.functionValue != null) return false;
		}
		else if (!functionValue.equals(other.functionValue)) return false;
		if (inverseExpression == null) {
			if (other.inverseExpression != null) return false;
		}
		else if (!inverseExpression.equals(other.inverseExpression)) return false;
		if (reference == null) {
			if (other.reference != null) return false;
		}
		else if (!reference.equals(other.reference)) return false;
		if (template == null) {
			if (other.template != null) return false;
		}
		else if (!template.equals(other.template)) return false;
		if (termType != other.termType) return false;
		return true;
	}

	public static class Builder {
		
		private String reference;
		private String inverseExpression;
		private String template;
		private TermType termType;
		private Value constant;
		private TriplesMap functionValue;
		
		Builder reference(String reference) {
			this.reference = reference;
			return this;
		}
		
		Builder inverseExpression(String inverseExpression) {
			this.inverseExpression = inverseExpression;
			return this;
		}
		
		Builder template(String template) {
			this.template = template;
			return this;
		}
		
		Builder termType(TermType termType) {
			this.termType = termType;
			return this;
		}
		
		Builder constant(Value constant) {
			this.constant = constant;
			return this;
		}
		
		Builder functionValue(TriplesMap functionValue) {
			this.functionValue = functionValue;
			return this;
		}

		String getReference() {
			return reference;
		}

		String getInverseExpression() {
			return inverseExpression;
		}

		String getTemplate() {
			return template;
		}
		
		TermType getTermType() {
			return termType;
		}

		Value getConstant() {
			return constant;
		}

		TriplesMap getFunctionValue() {
			return functionValue;
		}
	}
}
