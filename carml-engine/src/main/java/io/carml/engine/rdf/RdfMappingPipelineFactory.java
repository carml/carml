package io.carml.engine.rdf;

import static io.carml.logicalsourceresolver.MatchedLogicalSourceResolverSupplier.select;
import static io.carml.util.LogUtil.exception;
import static io.carml.util.LogUtil.log;
import static io.carml.vocab.Rml.referenceFormulation;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

import com.google.common.collect.Sets;
import io.carml.engine.MappingPipeline;
import io.carml.engine.RefObjectMapper;
import io.carml.engine.RmlMapperException;
import io.carml.engine.TriplesMapper;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverSupplier;
import io.carml.logicalsourceresolver.sql.sourceresolver.JoiningDatabaseSource;
import io.carml.model.DatabaseSource;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.model.impl.CarmlPredicateObjectMap;
import io.carml.util.Expressions;
import io.carml.vocab.Rdf;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;

@Slf4j
@NoArgsConstructor(staticName = "getInstance")
public class RdfMappingPipelineFactory {

  public MappingPipeline<Statement> getMappingPipeline(Set<TriplesMap> triplesMaps, RdfMapperConfig rdfMapperConfig,
      Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers,
      Set<MatchingLogicalSourceResolverSupplier> matchingLogicalSourceResolverSuppliers) {

    var tmToRoMappers = new HashMap<TriplesMap, Set<RdfRefObjectMapper>>();
    var roMapperToParentTm = new HashMap<RdfRefObjectMapper, TriplesMap>();

    prepareDatabaseTriplesMaps(triplesMaps);

    for (TriplesMap triplesMap : triplesMaps) {
      var roMappers = new HashSet<RdfRefObjectMapper>();
      triplesMap.getPredicateObjectMaps()
          .stream()
          .flatMap(pom -> pom.getObjectMaps()
              .stream())
          .filter(RefObjectMap.class::isInstance)
          .map(RefObjectMap.class::cast)
          .filter(rom -> !rom.getJoinConditions()
              .isEmpty() && !rom.isSelfJoining(triplesMap))
          .forEach(rom -> {
            if (!isTableJoiningRefObjectMap(rom, triplesMap)) {
              var roMapper = RdfRefObjectMapper.of(rom, triplesMap, rdfMapperConfig);
              roMappers.add(roMapper);
              roMapperToParentTm.put(roMapper, rom.getParentTriplesMap());
            }
          });
      tmToRoMappers.put(triplesMap, roMappers);
    }

    var parentTmToRoMappers = roMapperToParentTm.entrySet()
        .stream()
        .collect(groupingBy(Entry::getValue, mapping(Entry::getKey, toSet())));

    var tableJoiningGroups = getTableJoiningGroups(triplesMaps);
    var sourceToLogicalSourceResolver = buildLogicalSourceResolvers(triplesMaps, tableJoiningGroups,
        logicalSourceResolverSuppliers, matchingLogicalSourceResolverSuppliers);

    var triplesMapperStream = triplesMaps.stream()
        .map(triplesMap -> RdfTriplesMapper.of(triplesMap, tmToRoMappers.get(triplesMap),
            !parentTmToRoMappers.containsKey(triplesMap) ? Set.of() : parentTmToRoMappers.get(triplesMap),
            getTriplesMapLogicalSourceResolver(triplesMap, sourceToLogicalSourceResolver), rdfMapperConfig));

    var joiningTriplesMapperStream = tableJoiningGroups.stream()
        .map(tableJoiningGroup -> RdfJoiningTriplesMapper.of(tableJoiningGroup.getTriplesMap(),
            tableJoiningGroup.getReferencingPredicateObjectMaps(), tableJoiningGroup.getJoiningLogicalSource(),
            getTriplesMapLogicalSourceResolver(tableJoiningGroup.getTriplesMap(), sourceToLogicalSourceResolver),
            rdfMapperConfig));

    Set<TriplesMapper<Statement>> triplesMappers = Stream.concat(triplesMapperStream, joiningTriplesMapperStream)
        .collect(toUnmodifiableSet());

    Map<RefObjectMapper<Statement>, TriplesMapper<Statement>> roMapperToParentTriplesMapper = roMapperToParentTm
        .entrySet()
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(Entry::getKey, entry -> getTriplesMapper(entry.getValue(), triplesMappers)));

    return MappingPipeline.of(triplesMappers, roMapperToParentTriplesMapper, sourceToLogicalSourceResolver);
  }

  private void prepareDatabaseTriplesMaps(Set<TriplesMap> triplesMaps) {
    var groupedTriplesMaps = triplesMaps.stream()
        .filter(this::isDatabaseTriplesMap)
        .collect(groupingBy(TriplesMap::getLogicalSource, toUnmodifiableSet()));

    var lsExpressions = groupedTriplesMaps.entrySet()
        .stream()
        .collect(toUnmodifiableMap(Entry::getKey, entry -> entry.getValue()
            .stream()
            .map(Expressions::getExpressions)
            .flatMap(Set::stream)
            .collect(toUnmodifiableSet())));

    lsExpressions.forEach((logicalSource, expressions) -> groupedTriplesMaps.get(logicalSource)
        .forEach(triplesMap -> triplesMap.getLogicalSource()
            .setExpressions(expressions)));
  }

  private Set<TableJoiningGroup> getTableJoiningGroups(Set<TriplesMap> triplesMaps) {
    return triplesMaps.stream()
        .filter(this::isTableJoiningTriplesMap)
        .flatMap(this::toTableJoiningGroupStream)
        .collect(toUnmodifiableSet());
  }

  private Stream<TableJoiningGroup> toTableJoiningGroupStream(TriplesMap triplesMap) {

    var filteredPomsByParentLogicalSourceAndJoins = new HashMap<ParentLogicalSourceAndJoins, Set<PredicateObjectMap>>();
    for (var pom : triplesMap.getPredicateObjectMaps()) {
      var refObjectMapsByParentLogicalSourceAndJoins = pom.getObjectMaps()
          .stream()
          .filter(RefObjectMap.class::isInstance)
          .map(RefObjectMap.class::cast)
          .filter(rom -> !rom.getJoinConditions()
              .isEmpty() && !rom.isSelfJoining(triplesMap))
          .filter(rom -> isTableJoiningRefObjectMap(rom, triplesMap))
          .collect(groupingBy(rom -> ParentLogicalSourceAndJoins.of(rom.getParentTriplesMap()
              .getLogicalSource(), rom.getJoinConditions()), toUnmodifiableSet()));


      refObjectMapsByParentLogicalSourceAndJoins.entrySet()
          .stream()
          .map(refObjectMapsGroup -> Map.entry(refObjectMapsGroup.getKey(),
              Set.of((PredicateObjectMap) CarmlPredicateObjectMap.builder()
                  .graphMaps(pom.getGraphMaps())
                  .predicateMaps(pom.getPredicateMaps())
                  .objectMaps(refObjectMapsGroup.getValue())
                  .build())))
          .forEach(pomsByParentLogicalSource -> filteredPomsByParentLogicalSourceAndJoins
              .merge(pomsByParentLogicalSource.getKey(), pomsByParentLogicalSource.getValue(), Sets::union));
    }

    return filteredPomsByParentLogicalSourceAndJoins.entrySet()
        .stream()
        .map(pomsByLogicalSourceAndJoins -> toTableJoiningGroup(triplesMap, pomsByLogicalSourceAndJoins.getKey()
            .getParentLogicalSource(), pomsByLogicalSourceAndJoins.getValue()));
  }

  private TableJoiningGroup toTableJoiningGroup(TriplesMap triplesMap, LogicalSource parentLogicalSource,
      Set<PredicateObjectMap> predicateObjectMaps) {
    var refObjectMaps = predicateObjectMaps.stream()
        .map(PredicateObjectMap::getObjectMaps)
        .flatMap(Set::stream)
        .filter(RefObjectMap.class::isInstance)
        .map(RefObjectMap.class::cast)
        .collect(toUnmodifiableSet());

    var childExpressions = Expressions.getExpressions(triplesMap, predicateObjectMaps);

    var parentTmExpressions = refObjectMaps.stream()
        .map(RefObjectMap::getParentTriplesMap)
        .map(TriplesMap::getSubjectMaps)
        .flatMap(Set::stream)
        .map(Expressions::getExpressions)
        .flatMap(Set::stream);

    var joinParentExpressions = refObjectMaps.stream()
        .map(RefObjectMap::getJoinConditions)
        .flatMap(Set::stream)
        .map(Join::getParent);

    var parentExpressions = Stream.concat(parentTmExpressions, joinParentExpressions)
        .collect(toUnmodifiableSet());

    var joiningLogicalSource = CarmlLogicalSource.builder()
        .source(JoiningDatabaseSource.builder()
            .childLogicalSource(triplesMap.getLogicalSource())
            .parentLogicalSource(parentLogicalSource)
            .refObjectMaps(refObjectMaps)
            .childExpressions(childExpressions)
            .parentExpressions(parentExpressions)
            .build())
        .referenceFormulation(Rdf.Ql.Rdb)
        .build();

    return TableJoiningGroup.of(triplesMap, joiningLogicalSource, predicateObjectMaps);
  }

  private boolean isTableJoiningTriplesMap(TriplesMap triplesMap) {
    return triplesMap.getPredicateObjectMaps()
        .stream()
        .anyMatch(pom -> pom.getObjectMaps()
            .stream()
            .filter(RefObjectMap.class::isInstance)
            .map(RefObjectMap.class::cast)
            .filter(rom -> !rom.getJoinConditions()
                .isEmpty() && !rom.isSelfJoining(triplesMap))
            .anyMatch(rom -> isTableJoiningRefObjectMap(rom, triplesMap)));
  }

  private boolean isTableJoiningRefObjectMap(RefObjectMap refObjectMap, TriplesMap triplesMap) {
    return isDatabaseTriplesMap(triplesMap) && isDatabaseTriplesMap(refObjectMap.getParentTriplesMap())
        && isSameDatabase(triplesMap, refObjectMap.getParentTriplesMap());
  }

  private boolean isDatabaseTriplesMap(TriplesMap triplesMap) {
    return triplesMap.getLogicalTable() != null || triplesMap.getLogicalSource()
        .getSource() instanceof DatabaseSource;
  }

  private boolean isSameDatabase(TriplesMap child, TriplesMap parent) {
    var childSource = child.getLogicalSource()
        .getSource();
    var parentSource = parent.getLogicalSource()
        .getSource();
    if (childSource instanceof DatabaseSource childDbSource && parentSource instanceof DatabaseSource parentDbSource) {
      var childJdbcDsn = childDbSource.getJdbcDsn();
      var parentJdbcDsn = parentDbSource.getJdbcDsn();

      if (childJdbcDsn != null && parentJdbcDsn != null) {
        return childJdbcDsn.equals(parentJdbcDsn);
      }

      return childJdbcDsn == null && parentJdbcDsn == null;
    }

    return false;
  }

  private Map<Object, LogicalSourceResolver<?>> buildLogicalSourceResolvers(Set<TriplesMap> triplesMaps,
      Set<TableJoiningGroup> tableJoiningGroups,
      Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers,
      Set<MatchingLogicalSourceResolverSupplier> logicalSourceResolverMatchers) {

    if (triplesMaps.isEmpty()) {
      throw new RmlMapperException("No executable triples maps found.");
    }

    var triplesMapLogicalSources = triplesMaps.stream()
        .map(TriplesMap::getLogicalSource);

    var tableJoiningLogicalSources = tableJoiningGroups.stream()
        .map(TableJoiningGroup::getJoiningLogicalSource);

    var sourceToLogicalSources = Stream.concat(triplesMapLogicalSources, tableJoiningLogicalSources)
        .collect(groupingBy(LogicalSource::getSource, toSet()));

    return sourceToLogicalSources.entrySet()
        .stream()
        .collect(Collectors.toUnmodifiableMap(Entry::getKey, entry -> buildLogicalSourceResolver(entry.getValue(),
            triplesMaps, logicalSourceResolverSuppliers, logicalSourceResolverMatchers)));
  }

  private LogicalSourceResolver<?> buildLogicalSourceResolver(Set<LogicalSource> logicalSources,
      Set<TriplesMap> triplesMaps, Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers,
      Set<MatchingLogicalSourceResolverSupplier> logicalSourceResolverMatchers) {

    if (!logicalSourceResolverSuppliers.isEmpty()) {
      var resolver = logicalSources.stream()
          .map(LogicalSource::getReferenceFormulation)
          .findFirst()
          .map(referenceFormulation -> getLogicalSourceResolver(referenceFormulation, logicalSourceResolverSuppliers,
              logicalSourceResolverMatchers))
          .orElse(null);

      if (resolver != null) {
        return resolver;
      }
    }

    return logicalSources.stream()
        .findFirst()
        .map(logicalSource -> getLogicalSourceResolver(logicalSource, logicalSourceResolverMatchers))
        .orElseThrow(() -> new RmlMapperException(
            String.format("No logical sources found in triplesMaps:%n%s", exception(triplesMaps))));
  }

  private LogicalSourceResolver<?> getLogicalSourceResolver(IRI referenceFormulation,
      Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers,
      Set<MatchingLogicalSourceResolverSupplier> logicalSourceResolverMatchers) {
    var logicalSourceResolverSupplier = logicalSourceResolverSuppliers.get(referenceFormulation);

    if (logicalSourceResolverSupplier == null) {
      if (logicalSourceResolverMatchers.isEmpty()) {
        throw new RmlMapperException(String.format(
            "No logical source resolver supplier bound for reference formulation %s%nResolvers available: %s",
            referenceFormulation, logicalSourceResolverSuppliers.keySet()
                .stream()
                .map(IRI::stringValue)
                .collect(joining(", "))));
      }

      return null;
    }

    return logicalSourceResolverSupplier.get();
  }

  private LogicalSourceResolver<?> getLogicalSourceResolver(LogicalSource logicalSource,
      Set<MatchingLogicalSourceResolverSupplier> logicalSourceResolverMatchers) {

    var matchedLogicalSourceResolverSuppliers = logicalSourceResolverMatchers.stream()
        .map(matcher -> matcher.apply(logicalSource))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .toList();

    if (matchedLogicalSourceResolverSuppliers.size() > 1) {
      LOG.debug("Found multiple matching resolvers [{}] for logical source {}",
          matchedLogicalSourceResolverSuppliers.stream()
              .map(Object::getClass)
              .map(Class::getSimpleName),
          log(logicalSource));
    }

    return select(matchedLogicalSourceResolverSuppliers)
        .orElseThrow(() -> new RmlMapperException(String.format(
            "No logical source resolver supplier bound for reference formulation %s%nResolvers available: %s",
            referenceFormulation, logicalSourceResolverMatchers.stream()
                .map(MatchingLogicalSourceResolverSupplier::getResolverName)
                .collect(joining(", ")))))
        .get();
  }

  private LogicalSourceResolver<?> getTriplesMapLogicalSourceResolver(TriplesMap triplesMap,
      Map<Object, LogicalSourceResolver<?>> sourceToLogicalSourceResolver) {
    return sourceToLogicalSourceResolver.entrySet()
        .stream()
        .filter(entry -> entry.getKey()
            .equals(triplesMap.getLogicalSource()
                .getSource()))
        .map(Entry::getValue)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            String.format("LogicalSourceResolver not found for TriplesMap:%n%s", exception(triplesMap))));
  }

  private TriplesMapper<Statement> getTriplesMapper(TriplesMap triplesMap,
      Set<TriplesMapper<Statement>> triplesMappers) {
    return triplesMappers.stream()
        .filter(triplesMapper -> triplesMapper.getTriplesMap()
            .equals(triplesMap))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            String.format("TriplesMapper not found for TriplesMap:%n%s", exception(triplesMap))));
  }

  @AllArgsConstructor(staticName = "of")
  @Getter
  private static class TableJoiningGroup {

    @NonNull
    private TriplesMap triplesMap;

    @NonNull
    private LogicalSource joiningLogicalSource;

    @NonNull
    private Set<PredicateObjectMap> referencingPredicateObjectMaps;
  }

  @AllArgsConstructor(staticName = "of")
  @Getter
  private static class ParentLogicalSourceAndJoins {

    @NonNull
    private LogicalSource parentLogicalSource;

    @NonNull
    private Set<Join> joinConditions;
  }
}
