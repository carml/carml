package io.carml.model;

import java.util.List;
import java.util.Objects;
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

        var objectMaps = getObjectMaps().stream()
                .filter(ObjectMap.class::isInstance)
                .map(ObjectMap.class::cast)
                .toList();

        var objectMapExpressions = objectMaps.stream()
                .map(ObjectMap::getExpressionMapExpressionSet)
                .flatMap(Set::stream);

        var gatherExpressions = objectMaps.stream()
                .map(ObjectMap::getGathers)
                .flatMap(List::stream)
                .filter(ObjectMap.class::isInstance)
                .map(ObjectMap.class::cast)
                .map(ObjectMap::getExpressionMapExpressionSet)
                .flatMap(Set::stream);

        var languageMapExpressions = objectMaps.stream()
                .map(ObjectMap::getLanguageMap)
                .filter(Objects::nonNull)
                .map(LanguageMap::getExpressionMapExpressionSet)
                .flatMap(Set::stream);

        var datatypeMapExpressions = objectMaps.stream()
                .map(ObjectMap::getDatatypeMap)
                .filter(Objects::nonNull)
                .map(DatatypeMap::getExpressionMapExpressionSet)
                .flatMap(Set::stream);

        var refObjectMapExpressions = getObjectMaps().stream()
                .filter(RefObjectMap.class::isInstance)
                .map(RefObjectMap.class::cast)
                .map(RefObjectMap::getJoinConditions)
                .flatMap(Set::stream)
                .map(Join::getChildMap)
                .map(ChildMap::getExpressionMapExpressionSet)
                .flatMap(Set::stream);

        var graphMapExpressions = getGraphMaps().stream()
                .map(GraphMap::getExpressionMapExpressionSet)
                .flatMap(Set::stream);

        return Stream.of(
                        predicateMapExpressions,
                        objectMapExpressions,
                        gatherExpressions,
                        languageMapExpressions,
                        datatypeMapExpressions,
                        refObjectMapExpressions,
                        graphMapExpressions)
                .flatMap(s -> s)
                .collect(Collectors.toUnmodifiableSet());
    }
}
