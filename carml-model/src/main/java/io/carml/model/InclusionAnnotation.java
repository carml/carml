package io.carml.model;

import java.util.List;

public interface InclusionAnnotation extends StructuralAnnotation {

    LogicalView getTargetView();

    List<Field> getTargetFields();
}
