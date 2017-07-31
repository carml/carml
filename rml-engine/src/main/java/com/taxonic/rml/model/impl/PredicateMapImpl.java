package com.taxonic.rml.model.impl;

import org.eclipse.rdf4j.model.Resource;

import com.taxonic.rml.model.PredicateMap;

public class PredicateMapImpl extends TermMapImpl implements PredicateMap {

	public PredicateMapImpl() {}
	
	public PredicateMapImpl(
		String reference,
		String inverseExpression,
		String template,
		Object termType,
		Resource constant
	) {
		super(reference, inverseExpression, template, termType, constant);
	}

	@Override
	public String toString() {
		return "PredicateMapImpl [getReference()=" + getReference() + ", getInverseExpression()="
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

	
}

