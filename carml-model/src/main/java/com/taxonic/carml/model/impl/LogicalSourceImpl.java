package com.taxonic.carml.model.impl;

import org.eclipse.rdf4j.model.IRI;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Rml;

public class LogicalSourceImpl implements LogicalSource {

	private Object source;
	private String iterator;
	private IRI referenceFormulation;

	public LogicalSourceImpl() {}
	
	public LogicalSourceImpl(
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
		return "LogicalSourceImpl [getSource()=" + getSource() + ", getIterator()=" + getIterator()
			+ ", getReferenceFormulation()=" + getReferenceFormulation() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((iterator == null) ? 0 : iterator.hashCode());
		result = prime * result + ((referenceFormulation == null) ? 0 : referenceFormulation.hashCode());
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		LogicalSourceImpl other = (LogicalSourceImpl) obj;
		if (iterator == null) {
			if (other.iterator != null) return false;
		}
		else if (!iterator.equals(other.iterator)) return false;
		if (referenceFormulation == null) {
			if (other.referenceFormulation != null) return false;
		}
		else if (!referenceFormulation.equals(other.referenceFormulation)) return false;
		if (source == null) {
			if (other.source != null) return false;
		}
		else if (!source.equals(other.source)) return false;
		return true;
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

		public LogicalSourceImpl build() {
			return new LogicalSourceImpl(
				source,
				iterator,
				referenceFormulation
			);
		}
	}
}
