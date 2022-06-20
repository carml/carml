package io.carml.util;

import io.carml.model.ObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TermMap;
import io.carml.model.TriplesMap;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Mapping {

  private Mapping() {}

  public static Set<TriplesMap> filterMappable(Set<TriplesMap> mapping) {
    Set<TriplesMap> functionValueTriplesMaps = getTermMaps(mapping).map(TermMap::getFunctionValue)
        .filter(Objects::nonNull)
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
            Stream.concat(m.getSubjectMaps()
                .stream(),
                m.getSubjectMaps()
                    .stream()
                    .map(SubjectMap::getGraphMaps)
                    .flatMap(Set::stream))))
        .filter(Objects::nonNull);
  }

  private static Set<TriplesMap> getAllTriplesMapsUsedInRefObjectMap(Set<TriplesMap> mapping) {
    return mapping.stream()
        // get all referencing object maps
        .flatMap(m -> m.getPredicateObjectMaps()
            .stream())
        .flatMap(p -> p.getObjectMaps()
            .stream())
        .filter(RefObjectMap.class::isInstance)
        .map(RefObjectMap.class::cast)
        // check that no referencing object map
        // has 'map' as its parent triples map
        .map(RefObjectMap::getParentTriplesMap)
        .collect(Collectors.toUnmodifiableSet());
  }

}
