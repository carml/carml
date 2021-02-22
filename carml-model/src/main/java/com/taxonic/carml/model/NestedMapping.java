package com.taxonic.carml.model;

import java.util.Set;

public interface NestedMapping extends BaseObjectMap {

    TriplesMap getTriplesMap();

    Set<ContextEntry> getContextEntries();

}
