package io.carml.model;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface TriplesMap extends Resource {

  LogicalSource getLogicalSource();

  LogicalTable getLogicalTable();

  Set<SubjectMap> getSubjectMaps();

  Set<PredicateObjectMap> getPredicateObjectMaps();

  default Set<String> getReferenceExpressionSet() {
    return getReferenceExpressionSet(getPredicateObjectMaps());
  }

  default Set<String> getReferenceExpressionSet(Set<PredicateObjectMap> predicateObjectMapFilter) {
    var subjectMapExpressions = getSubjectMaps().stream()
        .map(SubjectMap::getReferenceExpressionSet)
        .flatMap(Set::stream);

    var pomExpressions = predicateObjectMapFilter.stream()
        .map(PredicateObjectMap::getReferenceExpressionSet)
        .flatMap(Set::stream);

    return Stream.concat(subjectMapExpressions, pomExpressions)
        .collect(Collectors.toUnmodifiableSet());
  }
}
