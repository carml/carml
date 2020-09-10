package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;
import com.taxonic.carml.vocab.Rdf;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlStream extends CarmlResource implements NameableStream {

	private String streamName;

	public CarmlStream() {
		// Empty constructor for object mapper
	}

	public CarmlStream(String streamName) {
		this.streamName = streamName;
	}

	@RdfProperty(Carml.streamName)
	@Override
	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	@Override
	public int hashCode() {
		return Objects.hash(streamName);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NameableStream) {
			NameableStream other = (NameableStream) obj;
			return Objects.equals(streamName, other.getStreamName());
		}
		return false;
	}

	@Override
	public Set<Resource> getReferencedResources() {
		return ImmutableSet.of();
	}

	@Override
	public void addTriples(ModelBuilder modelBuilder) {
		modelBuilder.subject(getAsResource())
				.add(RDF.TYPE, Rdf.Carml.Stream);

		if (streamName != null) {
			modelBuilder.add(Carml.streamName, streamName);
		}
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}
}
