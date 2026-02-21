package io.carml.model;

import java.util.Set;

public interface Field extends Resource {

    String getFieldName();

    Set<Field> getFields();
}
