package io.carml.model;

import java.util.List;

public interface StructuralAnnotation extends Resource {

    List<Field> getOnFields();
}
