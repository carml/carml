package io.carml.model;

public interface Join extends Resource {

    ChildMap getChildMap();

    ParentMap getParentMap();
}
