package com.taxonic.carml.model.impl;

import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfResourceName;

public abstract class CarmlResource {

	String _id;
	String _label;

	@RdfResourceName
	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}

	@RdfProperty("http://www.w3.org/2000/01/rdf-schema#label")
	public String get_label() {
		return _label;
	}

	public void set_label(String _label) {
		this._label = _label;
	}
}
