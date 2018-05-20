package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.FileSource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;
import java.util.Objects;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class CarmlFileSource extends CarmlResource implements FileSource {

	private String url;

	public CarmlFileSource() {}

	@RdfProperty(Carml.url)
	@Override
	public String getUrl() {
		return this.url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(url);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof FileSource) {
			FileSource other = (FileSource) obj;
			return Objects.equals(url, other.getUrl());
		}
		return false;
	}

}
