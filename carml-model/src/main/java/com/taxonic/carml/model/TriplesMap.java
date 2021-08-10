package com.taxonic.carml.model;

import java.util.Set;

public interface TriplesMap extends Resource {

  LogicalSource getLogicalSource();

  Set<SubjectMap> getSubjectMaps();

  Set<PredicateObjectMap> getPredicateObjectMaps();

}
