package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Rml;
import java.util.Objects;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.IRI;

public class CarmlLogicalSource implements LogicalSource {

	private Object source;
	private String iterator;
	private IRI referenceFormulation;

	public CarmlLogicalSource() {}

	public CarmlLogicalSource(
		Object source,
		String iterator,
		IRI referenceFormulation
	) {
		this.source = source;
		this.iterator = iterator;
		this.referenceFormulation = referenceFormulation;
	}

	@RdfProperty(
		value = Rml.source,
		handler = LogicalSourceSourcePropertyHandler.class
	)
	@Override
	public Object getSource() {
		return source;
	}

	@RdfProperty(Rml.iterator)
	@Override
	public String getIterator() {
		return iterator;
	}

	@RdfProperty(Rml.referenceFormulation)
	@Override
	public IRI getReferenceFormulation() {
		return referenceFormulation;
	}

	public void setSource(Object source) {
		this.source = source;
	}

	public void setIterator(String iterator) {
		this.iterator = iterator;
	}

	public void setReferenceFormulation(IRI referenceFormulation) {
		this.referenceFormulation = referenceFormulation;
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(source, iterator, referenceFormulation);
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
		CarmlLogicalSource other = (CarmlLogicalSource) obj;
		return Objects.equals(source, other.source) &&
				Objects.equals(iterator, other.iterator) &&
				Objects.equals(referenceFormulation, other.referenceFormulation);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {

		private Object source;
		private String iterator;
		private IRI referenceFormulation;

		public Builder source(Object source) {
			this.source = source;
			return this;
		}

		public Builder iterator(String iterator) {
			this.iterator = iterator;
			return this;
		}

		public Builder referenceFormulation(IRI referenceFormulation) {
			this.referenceFormulation = referenceFormulation;
			return this;
		}

		public CarmlLogicalSource build() {
			return new CarmlLogicalSource(
				source,
				iterator,
				referenceFormulation
			);
		}
	}
}
