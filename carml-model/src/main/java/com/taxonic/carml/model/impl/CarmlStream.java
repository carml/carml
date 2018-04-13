package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;
import java.util.Objects;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class CarmlStream extends CarmlResource implements NameableStream {

	private String streamName;

	public CarmlStream() {}

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
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

}
