package io.carml.engine.rdf;

import static io.carml.util.LogUtil.exception;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import io.carml.engine.MappingPipeline;
import io.carml.engine.RefObjectMapper;
import io.carml.engine.RmlMapperException;
import io.carml.engine.TriplesMapper;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.model.LogicalSource;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;

@NoArgsConstructor(staticName = "getInstance")
public class RdfMappingPipelineFactory {

  public MappingPipeline<Statement> getMappingPipeline(Set<TriplesMap> triplesMaps, RdfMapperConfig rdfMapperConfig,
      Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers) {

    Map<TriplesMap, Set<RdfRefObjectMapper>> tmToRoMappers = new HashMap<>();
    Map<RdfRefObjectMapper, TriplesMap> roMapperToParentTm = new HashMap<>();

    if (triplesMaps.isEmpty()) {
      throw new RmlMapperException("No actionable triples maps provided.");
    }

    for (TriplesMap triplesMap : triplesMaps) {
      Set<RdfRefObjectMapper> roMappers = new HashSet<>();
      triplesMap.getPredicateObjectMaps()
          .stream()
          .flatMap(pom -> pom.getObjectMaps()
              .stream())
          .filter(RefObjectMap.class::isInstance)
          .map(RefObjectMap.class::cast)
          .filter(rom -> !rom.getJoinConditions()
              .isEmpty())
          .forEach(rom -> {
            var roMapper = RdfRefObjectMapper.of(rom, triplesMap, rdfMapperConfig);
            roMappers.add(roMapper);
            roMapperToParentTm.put(roMapper, rom.getParentTriplesMap());
          });
      tmToRoMappers.put(triplesMap, roMappers);
    }

    var parentTmToRoMappers = roMapperToParentTm.entrySet()
        .stream()
        .collect(groupingBy(Map.Entry::getValue, mapping(Map.Entry::getKey, toSet())));

    var sourceToLogicalSourceResolver = buildLogicalSourceResolvers(triplesMaps, logicalSourceResolverSuppliers);

    Set<TriplesMapper<Statement>> triplesMappers = triplesMaps.stream()
        .map(triplesMap -> RdfTriplesMapper.of(triplesMap, tmToRoMappers.get(triplesMap),
            !parentTmToRoMappers.containsKey(triplesMap) ? Set.of() : parentTmToRoMappers.get(triplesMap),
            getExpressionEvaluationFactory(triplesMap, sourceToLogicalSourceResolver), rdfMapperConfig))
        .collect(Collectors.toUnmodifiableSet());

    Map<RefObjectMapper<Statement>, TriplesMapper<Statement>> roMapperToParentTriplesMapper =
        roMapperToParentTm.entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                entry -> getTriplesMapper(entry.getValue(), triplesMappers)));

    return MappingPipeline.of(triplesMappers, roMapperToParentTriplesMapper, sourceToLogicalSourceResolver);
  }

  private Map<Object, LogicalSourceResolver<?>> buildLogicalSourceResolvers(Set<TriplesMap> triplesMaps,
      Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers) {

    if (triplesMaps.isEmpty()) {
      throw new RmlMapperException("No executable triples maps found.");
    }

    var sourceToLogicalSources = triplesMaps.stream()
        .map(TriplesMap::getLogicalSource)
        .collect(groupingBy(LogicalSource::getSource, toSet()));

    return sourceToLogicalSources.entrySet()
        .stream()
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
            entry -> buildLogicalSourceResolver(entry.getValue(), triplesMaps, logicalSourceResolverSuppliers)));
  }

  private LogicalSourceResolver<?> buildLogicalSourceResolver(Set<LogicalSource> logicalSources,
      Set<TriplesMap> triplesMaps, Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers) {
    var referenceFormulation = logicalSources.stream()
        .map(LogicalSource::getReferenceFormulation)
        .findFirst();

    return referenceFormulation
        .map(logicalSource -> getLogicalSourceResolver(logicalSource, logicalSourceResolverSuppliers))
        .orElseThrow(() -> new RmlMapperException(
            String.format("No logical sources found in triplesMaps:%n%s", exception(triplesMaps))));
  }

  private LogicalSourceResolver<?> getLogicalSourceResolver(IRI referenceFormulation,
      Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers) {
    var logicalSourceResolverSupplier = logicalSourceResolverSuppliers.get(referenceFormulation);

    if (logicalSourceResolverSupplier == null) {
      throw new RmlMapperException(String.format(
          "No logical source resolver supplier bound for reference formulation %s%nResolvers available: %s",
          referenceFormulation, logicalSourceResolverSuppliers.keySet()
              .stream()
              .map(IRI::stringValue)
              .collect(joining(", "))));
    }

    return logicalSourceResolverSupplier.get();
  }

  private LogicalSourceResolver.ExpressionEvaluationFactory<?> getExpressionEvaluationFactory(TriplesMap triplesMap,
      Map<Object, LogicalSourceResolver<?>> sourceToLogicalSourceResolver) {
    return sourceToLogicalSourceResolver.entrySet()
        .stream()
        .filter(entry -> entry.getKey()
            .equals(triplesMap.getLogicalSource()
                .getSource()))
        .map(Map.Entry::getValue)
        .map(LogicalSourceResolver::getExpressionEvaluationFactory)
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

}
