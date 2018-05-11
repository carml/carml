package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfResourceName;

public abstract class CarmlResource implements Resource {

	String id;
	String label;

	@Override
	@RdfResourceName
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	@RdfProperty("http://www.w3.org/2000/01/rdf-schema#label")
	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}
}
