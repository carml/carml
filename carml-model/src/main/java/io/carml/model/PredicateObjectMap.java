package io.carml.model;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface PredicateObjectMap extends Resource {

  Set<PredicateMap> getPredicateMaps();

  Set<BaseObjectMap> getObjectMaps();

  Set<GraphMap> getGraphMaps();

  default Set<String> getReferenceExpressionSet() {
    var predicateMapExpressions = getPredicateMaps().stream()
        .map(PredicateMap::getExpressionMapExpressionSet)
        .flatMap(Set::stream);

    var objectMapExpressions = getObjectMaps().stream()
        .filter(ObjectMap.class::isInstance)
        .map(ObjectMap.class::cast)
        .map(ObjectMap::getExpressionMapExpressionSet)
        .flatMap(Set::stream);

    var refObjectMapExpressions = getObjectMaps().stream()
        .filter(RefObjectMap.class::isInstance)
        .map(RefObjectMap.class::cast)
        .map(RefObjectMap::getJoinConditions)
        .flatMap(Set::stream)
        .map(Join::getChild);

    var graphMapExpressions = getGraphMaps().stream()
        .map(GraphMap::getExpressionMapExpressionSet)
        .flatMap(Set::stream);

    return Stream
        .concat(predicateMapExpressions,
            Stream.concat(objectMapExpressions, Stream.concat(refObjectMapExpressions, graphMapExpressions)))
        .collect(Collectors.toUnmodifiableSet());
  }
}
