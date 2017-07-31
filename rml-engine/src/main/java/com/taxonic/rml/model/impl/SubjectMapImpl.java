package com.taxonic.rml.model.impl;

import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;

import com.taxonic.rdf_mapper.annotations.RdfProperty;
import com.taxonic.rml.model.SubjectMap;
import com.taxonic.rml.vocab.Rr;

public class SubjectMapImpl extends TermMapImpl implements SubjectMap {

	private Set<IRI> classes;

	public SubjectMapImpl() {}
	
	public SubjectMapImpl(
		String reference,
		String inverseExpression,
		String template,
		Object termType,
		Resource constant,
		Set<IRI> classes
	) {
		super(reference, inverseExpression, template, termType, constant);
		this.classes = classes;
	}

	@RdfProperty(Rr.clazz)
	@Override
	public Set<IRI> getClasses() {
		return classes;
	}

	public void setClasses(Set<IRI> classes) {
		this.classes = classes;
	}

	@Override
	public String toString() {
		return "SubjectMapImpl [getClasses()=" + getClasses() + ", getReference()=" + getReference()
			+ ", getInverseExpression()=" + getInverseExpression() + ", getTemplate()=" + getTemplate()
			+ ", getTermType()=" + getTermType() + ", getConstant()=" + getConstant() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((classes == null) ? 0 : classes.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (getClass() != obj.getClass()) return false;
		SubjectMapImpl other = (SubjectMapImpl) obj;
		if (classes == null) {
			if (other.classes != null) return false;
		}
		else if (!classes.equals(other.classes)) return false;
		return true;
	}

}
