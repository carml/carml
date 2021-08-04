package com.taxonic.carml.util;

import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TermMap;
import com.taxonic.carml.model.TriplesMap;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Mapping {

  public static Set<TriplesMap> filterMappable(Set<TriplesMap> mapping) {
    Set<TriplesMap> functionValueTriplesMaps = getTermMaps(mapping).filter(t -> t.getFunctionValue() != null)
        .map(TermMap::getFunctionValue)
        .collect(Collectors.toUnmodifiableSet());

    Set<TriplesMap> refObjectTriplesMaps = getAllTriplesMapsUsedInRefObjectMap(mapping);

    return mapping.stream()
        .filter(m -> !functionValueTriplesMaps.contains(m) || refObjectTriplesMaps.contains(m))
        .collect(Collectors.toUnmodifiableSet());
  }

  private static Stream<TermMap> getTermMaps(Set<TriplesMap> mapping) {
    return mapping.stream()
        .flatMap(m -> Stream.concat(m.getPredicateObjectMaps()
            .stream()
            .flatMap(p -> Stream.concat(p.getGraphMaps()
                .stream(),
                Stream.concat(p.getPredicateMaps()
                    .stream(),
                    p.getObjectMaps()
                        .stream()
                        .filter(ObjectMap.class::isInstance)
                        .map(ObjectMap.class::cast)))),
            Stream.concat(Stream.of(m.getSubjectMap()), m.getSubjectMap() != null ? m.getSubjectMap()
                .getGraphMaps()
                .stream() : Stream.empty())))
        .filter(Objects::nonNull);
  }

  private static Set<TriplesMap> getAllTriplesMapsUsedInRefObjectMap(Set<TriplesMap> mapping) {
    return mapping.stream()
        // get all referencing object maps
        .flatMap(m -> m.getPredicateObjectMaps()
            .stream())
        .flatMap(p -> p.getObjectMaps()
            .stream())
        .filter(o -> o instanceof RefObjectMap)
        .map(o -> (RefObjectMap) o)

        // check that no referencing object map
        // has 'map' as its parent triples map
        .map(RefObjectMap::getParentTriplesMap)
        .collect(Collectors.toUnmodifiableSet());
  }

}
