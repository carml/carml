package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.CarmlStream;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;

public class CarmlStreamImpl implements CarmlStream {

	private String streamName;
	
	public CarmlStreamImpl() {}
	
	public CarmlStreamImpl(String streamName) {
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
		final int prime = 31;
		int result = 1;
		result = prime * result + ((streamName == null) ? 0 : streamName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		CarmlStreamImpl other = (CarmlStreamImpl) obj;
		if (streamName == null) {
			if (other.streamName != null) return false;
		}
		else if (!streamName.equals(other.streamName)) return false;
		return true;
	}

	@Override
	public String toString() {
		return "CarmlStreamImpl [streamName=" + streamName + "]";
	}

}
