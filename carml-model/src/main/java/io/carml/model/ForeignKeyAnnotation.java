package io.carml.model;

import java.util.List;

public interface ForeignKeyAnnotation extends StructuralAnnotation {

    LogicalView getTargetView();

    List<Field> getTargetFields();
}
