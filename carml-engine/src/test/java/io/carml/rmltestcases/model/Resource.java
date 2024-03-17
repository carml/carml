package io.carml.rmltestcases.model;

public interface Resource {

    String getId();

    void setId(String id);

    String getLabel();

    default String getResourceName() {
        return getLabel() != null ? "\"" + getLabel() + "\"" : "<" + getId() + ">";
    }
}
