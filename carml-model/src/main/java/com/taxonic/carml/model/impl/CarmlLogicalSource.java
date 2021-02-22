package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.MergeSuper;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.CarmlExp;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rml;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class CarmlLogicalSource extends CarmlResource implements LogicalSource {

	private Object source;
	private String iterator;
	private IRI referenceFormulation;
	private MergeSuper mergeSuper;

	public CarmlLogicalSource() {
		// Empty constructor for object mapper
	}

	public CarmlLogicalSource(
		Object source,
		String iterator,
		IRI referenceFormulation
	) {
		this(source, iterator, referenceFormulation, null);
	}

	public CarmlLogicalSource(
		Object source,
		String iterator,
		IRI referenceFormulation,
		MergeSuper mergeSuper
	) {
		this.source = source;
		this.iterator = iterator;
		this.referenceFormulation = referenceFormulation;
		this.mergeSuper = mergeSuper;
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

	@RdfProperty(CarmlExp.mergeSuper)
	@RdfType(CarmlMergeSuper.class)
	@Override
	public MergeSuper getMergeSuper() {
		return mergeSuper;
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

	public void setMergeSuper(MergeSuper mergeSuper) {
		this.mergeSuper = mergeSuper;
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(source, iterator, referenceFormulation, mergeSuper);
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
				Objects.equals(referenceFormulation, other.referenceFormulation) &&
				Objects.equals(mergeSuper, other.mergeSuper);
	}

	@Override
	public Set<Resource> getReferencedResources() {
		ImmutableSet.Builder<Resource> builder = ImmutableSet.builder();
		if (source instanceof Resource) {
			builder.add((Resource) source);
		}
		if (mergeSuper != null) {
			builder.add(mergeSuper);
		}
		return builder.build();
	}

	@Override
	public void addTriples(ModelBuilder modelBuilder) {
		modelBuilder.subject(getAsResource())
				.add(RDF.TYPE, Rdf.Rml.LogicalSource);
		if (source != null) {
			if (source instanceof Resource) {
				modelBuilder.add(Rml.source, ((Resource) source).getAsResource());
			} else {
				modelBuilder.add(Rml.source, source);
			}
		}
		if (iterator != null) {
			modelBuilder.add(Rml.iterator, iterator);
		}
		if (referenceFormulation != null) {
			modelBuilder.add(Rml.referenceFormulation, referenceFormulation);
		}
		if (mergeSuper != null) {
			modelBuilder.add(Rdf.CarmlExp.mergeSuper, mergeSuper.getAsResource());
		}
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {

		private Object source;
		private String iterator;
		private IRI referenceFormulation;
		private MergeSuper mergeSuper;

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

		public Builder mergeSuper(MergeSuper mergeSuper) {
			this.mergeSuper = mergeSuper;
			return this;
		}

		public CarmlLogicalSource build() {
			return new CarmlLogicalSource(
				source,
				iterator,
				referenceFormulation,
				mergeSuper
			);
		}
	}
}
