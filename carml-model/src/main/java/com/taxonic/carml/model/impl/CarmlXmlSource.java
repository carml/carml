package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.Namespace;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.XmlSource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Carml;
import com.taxonic.carml.vocab.Rdf;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlXmlSource extends CarmlResource implements XmlSource {

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
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(declaredNamespaces);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof XmlSource) {
			XmlSource other = (XmlSource) obj;
			return Objects.equals(declaredNamespaces, other.getDeclaredNamespaces());
		}
		return false;
	}

	@Override
	public Set<Resource> getReferencedResources() {
		return ImmutableSet.copyOf(declaredNamespaces);
	}

	@Override
	public void addTriples(ModelBuilder modelBuilder) {
		modelBuilder.subject(getAsResource())
				.add(RDF.TYPE, Rdf.Carml.XmlDocument);

		declaredNamespaces.forEach(ns -> modelBuilder.add(Carml.declaresNamespace, ns.getAsResource()));
	}

}
