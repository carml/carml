package io.carml.model;

import java.util.Set;

public interface TriplesMap extends Resource {

  LogicalSource getLogicalSource();

  LogicalTable getLogicalTable();

  Set<SubjectMap> getSubjectMaps();

  Set<PredicateObjectMap> getPredicateObjectMaps();

}
