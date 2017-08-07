package com.taxonic.rml.model.impl;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

import com.taxonic.rml.model.ObjectMap;
import com.taxonic.rml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.rml.vocab.Rr;

public class ObjectMapImpl extends TermMapImpl implements ObjectMap {

	private IRI datatype;
	private String language;
	
	public ObjectMapImpl() {}
	
	public ObjectMapImpl(
		String reference,
		String inverseExpression,
		String template,
		IRI termType,
		Value constant,
		IRI datatype,
		String language
	) {
		super(reference, inverseExpression, template, termType, constant);
		this.datatype = datatype;
		this.language = language;
	}

	@RdfProperty(Rr.datatype)
	@Override
	public IRI getDatatype() {
		return datatype;
	}

	@RdfProperty(Rr.language)
	@Override
	public String getLanguage() {
		return language;
	}

	public void setDatatype(IRI datatype) {
		this.datatype = datatype;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	@Override
	public String toString() {
		return "ObjectMapImpl [getDatatype()=" + getDatatype() + ", getLanguage()=" + getLanguage()
			+ ", getReference()=" + getReference() + ", getInverseExpression()=" + getInverseExpression()
			+ ", getTemplate()=" + getTemplate() + ", getTermType()=" + getTermType() + ", getConstant()="
			+ getConstant() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((datatype == null) ? 0 : datatype.hashCode());
		result = prime * result + ((language == null) ? 0 : language.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (getClass() != obj.getClass()) return false;
		ObjectMapImpl other = (ObjectMapImpl) obj;
		if (datatype == null) {
			if (other.datatype != null) return false;
		}
		else if (!datatype.equals(other.datatype)) return false;
		if (language == null) {
			if (other.language != null) return false;
		}
		else if (!language.equals(other.language)) return false;
		return true;
	}

	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder
		extends com.taxonic.rml.model.impl.TermMapImpl.Builder {
		
		private IRI datatype;
		private String language;

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
		
		public Builder termType(IRI termType) {
			super.termType(termType);
			return this;
		}
		
		public Builder constant(Value constant) {
			super.constant(constant);
			return this;
		}
		
		public Builder datatype(IRI datatype) {
			this.datatype = datatype;
			return this;
		}
		
		public Builder language(String language) {
			this.language = language;
			return this;
		}
		
		public ObjectMapImpl build() {
			return new ObjectMapImpl(
				getReference(),
				getInverseExpression(),
				getTemplate(),
				getTermType(),
				getConstant(),
				datatype,
				language
			);
		}
	}	
}
