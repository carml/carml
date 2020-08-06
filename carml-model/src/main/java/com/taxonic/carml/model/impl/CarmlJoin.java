package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rr;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlJoin extends CarmlResource implements Join{

	private String child;
	private String parent;

	public CarmlJoin() {}

	public CarmlJoin(
			String child,
			String parent
	) {
		this.child = child;
		this.parent = parent;
	}

	@RdfProperty(Rr.child)
	@Override
	public String getChildReference() {
		return child;
	}

	public void setChildReference(String child) {
		this.child = child;
	}

	@RdfProperty(Rr.parent)
	@Override
	public String getParentReference() {
		return parent;
	}

	public void setParentReference(String parent) {
		this.parent = parent;
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(child, parent);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		CarmlJoin other = (CarmlJoin) obj;
		return Objects.equals(child, other.child) && Objects.equals(parent, other.parent);
	}

	@Override
	public Set<Resource> getReferencedResources() {
		return ImmutableSet.of();
	}

	@Override
	public void addTriples(ModelBuilder modelBuilder) {
		modelBuilder.subject(getAsResource())
				.add(RDF.TYPE, Rdf.Rr.Join);
		if (child != null) {
			modelBuilder.add(Rr.child, child);
		}
		if (parent != null) {
			modelBuilder.add(Rr.parent, parent);
		}
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder{

		private String child;
		private String parent;

		Builder() {}

		public Builder child(String child) {
			this.child = child;
			return this;
		}

		public Builder parent(String parent) {
			this.parent = parent;
			return this;
		}

		public CarmlJoin build() {
			return new CarmlJoin(
				child,
				parent
			);
		}
	}
}
