package com.taxonic.carml.rmltestcases.model;

public interface Resource {

	String getId();

	String getLabel();

	default String getResourceName() {
		return getLabel() != null ? "\"" + getLabel() + "\"" : "<" + getId() + ">";
	}

}
