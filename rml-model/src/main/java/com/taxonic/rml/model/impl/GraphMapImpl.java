package com.taxonic.rml.model.impl;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

import com.taxonic.rml.model.GraphMap;

public class GraphMapImpl extends TermMapImpl implements GraphMap{

		public GraphMapImpl() {}
		
		public GraphMapImpl(
			String reference,
			String inverseExpression,
			String template,
			IRI termType,
			Value constant
		) {
			super(reference, inverseExpression, template, termType, constant);
		}

		@Override
		public String toString() {
			return "GraphMapImpl [getReference()=" + getReference() + ", getInverseExpression()="
				+ getInverseExpression() + ", getTemplate()=" + getTemplate() + ", getTermType()=" + getTermType()
				+ ", getConstant()=" + getConstant() + "]";
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
			
			public GraphMapImpl build() {
				return new GraphMapImpl(
					getReference(),
					getInverseExpression(),
					getTemplate(),
					getTermType(),
					getConstant()
				);
			}
		}
}

