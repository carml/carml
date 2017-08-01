package com.taxonic.rml.model.impl;

import org.eclipse.rdf4j.model.Value;

import com.taxonic.rml.model.TermMap;
import com.taxonic.rml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.rml.vocab.Rml;
import com.taxonic.rml.vocab.Rr;

abstract public class TermMapImpl implements TermMap {

	private String reference;
	private String inverseExpression;
	private String template;
	private Object termType;
	private Value constant;

	public TermMapImpl() {}
	
	public TermMapImpl(
		String reference,
		String inverseExpression,
		String template,
		Object termType,
		Value constant
	) {
		this.reference = reference;
		this.inverseExpression = inverseExpression;
		this.template = template;
		this.termType = termType;
		this.constant = constant;
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
//	@RdfProperty(Rr.termType)
	@Override
	public Object getTermType() {
		return termType;
	}

	@RdfProperty(Rr.constant)
	@Override
	public Value getConstant() {
		return constant;
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

	public void setTermType(Object termType) {
		this.termType = termType;
	}

	public void setConstant(Value constant) {
		this.constant = constant;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((constant == null) ? 0 : constant.hashCode());
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
		if (termType == null) {
			if (other.termType != null) return false;
		}
		else if (!termType.equals(other.termType)) return false;
		return true;
	}

	public static class Builder {
		
		private String reference;
		private String inverseExpression;
		private String template;
		private Object termType;
		private Value constant;
		
		Builder() {}
		
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
		
		Builder termType(Object termType) {
			this.termType = termType;
			return this;
		}
		
		Builder constant(Value constant) {
			this.constant = constant;
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

		Object getTermType() {
			return termType;
		}

		Value getConstant() {
			return constant;
		}
	}
}
