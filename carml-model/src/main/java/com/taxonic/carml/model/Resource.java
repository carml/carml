package com.taxonic.carml.model;

public interface Resource {

	String getId();

	String getLabel();

	default String getResourceName() {
		return getLabel() != null ? "\"" + getLabel() + "\"" : "<" + getId() + ">";
	}

}
