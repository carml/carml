package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.FileSource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;
import java.util.Objects;

public class CarmlFileSource implements FileSource {

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
		return "CarmlFileSource [url=" + url + "]";
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
