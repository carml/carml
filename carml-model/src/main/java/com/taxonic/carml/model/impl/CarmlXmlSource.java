package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Namespace;
import com.taxonic.carml.model.XmlSource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Carml;
import java.util.LinkedHashSet;
import java.util.Set;

public class CarmlXmlSource implements XmlSource {

	private Set<Namespace> declaredNamespaces;
	
	public CarmlXmlSource() {
		this.declaredNamespaces = new LinkedHashSet<>();
	}
	
	public CarmlXmlSource(Set<Namespace> declaredNamespaces) {
		this.declaredNamespaces = declaredNamespaces;
	}

	@RdfProperty(Carml.declaresNamespace)
	@RdfType(CarmlNamespace.class)
	@Override
	public Set<Namespace> getDeclaredNamespaces() {
		return declaredNamespaces;
	}

	public void setDeclaredNamespaces(Set<Namespace> declaredNamespaces) {
		this.declaredNamespaces = declaredNamespaces;
	}

	@Override
	public String toString() {
		return "CarmlXmlSource [declaredNamespaces=" + declaredNamespaces + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((declaredNamespaces == null) ? 0 : declaredNamespaces.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CarmlXmlSource other = (CarmlXmlSource) obj;
		if (declaredNamespaces == null) {
			if (other.declaredNamespaces != null)
				return false;
		} else if (!declaredNamespaces.equals(other.declaredNamespaces))
			return false;
		return true;
	}

}
