package io.carml.model;

import java.util.Set;

public interface LogicalView extends AbstractLogicalSource {

    AbstractLogicalSource getViewOn();

    Set<Field> getFields();

    Set<LogicalViewJoin> getLeftJoins();

    Set<LogicalViewJoin> getInnerJoins();

    Set<StructuralAnnotation> getStructuralAnnotations();
}
