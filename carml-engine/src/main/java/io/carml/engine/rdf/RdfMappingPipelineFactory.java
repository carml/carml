package io.carml.engine.rdf;

import static io.carml.util.LogUtil.exception;
import static io.carml.util.LogUtil.log;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.engine.MappingPipeline;
import io.carml.engine.RmlMapperException;
import io.carml.engine.TriplesMapper;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.TriplesMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;

@Slf4j
@NoArgsConstructor(staticName = "getInstance")
public class RdfMappingPipelineFactory {

    public MappingPipeline<Statement> getMappingPipeline(
            Set<TriplesMap> triplesMaps,
            RdfMapperConfig rdfMapperConfig,
            Set<MatchingLogicalSourceResolverFactory> matchingLogicalSourceResolverFactories) {

        var expressionsPerLogicalSource = collectExpressionsPerLogicalSource(triplesMaps);

        var sourceToLogicalSourceResolver =
                buildLogicalSourceResolvers(triplesMaps, matchingLogicalSourceResolverFactories);

        Set<TriplesMapper<Statement>> triplesMappers = triplesMaps.stream()
                .map(triplesMap -> RdfTriplesMapper.of(
                        triplesMap,
                        getTriplesMapLogicalSourceResolver(triplesMap, sourceToLogicalSourceResolver),
                        getEffectiveMapperConfig(triplesMap, rdfMapperConfig)))
                .collect(toUnmodifiableSet());

        return MappingPipeline.of(triplesMappers, sourceToLogicalSourceResolver, expressionsPerLogicalSource);
    }

    private Map<LogicalSource, Set<String>> collectExpressionsPerLogicalSource(Set<TriplesMap> triplesMaps) {
        var groupedTriplesMaps =
                triplesMaps.stream().collect(groupingBy(TriplesMap::getLogicalSource, toUnmodifiableSet()));

        return groupedTriplesMaps.entrySet().stream()
                .filter(entry -> entry.getKey() instanceof LogicalSource)
                .collect(toUnmodifiableMap(entry -> (LogicalSource) entry.getKey(), entry -> entry.getValue().stream()
                        .map(TriplesMap::getReferenceExpressionSet)
                        .flatMap(Set::stream)
                        .collect(toUnmodifiableSet())));
    }

    private Map<Source, LogicalSourceResolver<?>> buildLogicalSourceResolvers(
            Set<TriplesMap> triplesMaps,
            Set<MatchingLogicalSourceResolverFactory> matchingLogicalSourceResolverFactories) {

        if (triplesMaps.isEmpty()) {
            throw new RmlMapperException("No executable triples maps found.");
        }

        var sourceToLogicalSources = triplesMaps.stream()
                .map(RdfMappingPipelineFactory::requireLogicalSource)
                .collect(groupingBy(LogicalSource::getSource, toSet()));

        return sourceToLogicalSources.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Entry::getKey,
                        entry -> buildLogicalSourceResolver(
                                entry.getValue(), triplesMaps, matchingLogicalSourceResolverFactories)));
    }

    private LogicalSourceResolver<?> buildLogicalSourceResolver(
            Set<LogicalSource> logicalSources,
            Set<TriplesMap> triplesMaps,
            Set<MatchingLogicalSourceResolverFactory> matchingLogicalSourceResolverFactories) {

        return logicalSources.stream()
                .findFirst()
                .map(logicalSource -> getLogicalSourceResolver(logicalSource, matchingLogicalSourceResolverFactories))
                .orElseThrow(() -> new RmlMapperException(
                        String.format("No logical sources found in triplesMaps:%n%s", exception(triplesMaps))));
    }

    private LogicalSourceResolver<?> getLogicalSourceResolver(
            LogicalSource logicalSource,
            Set<MatchingLogicalSourceResolverFactory> matchingLogicalSourceResolverFactories) {

        var matchedLogicalSourceResolverSuppliers = matchingLogicalSourceResolverFactories.stream()
                .map(matcher -> matcher.apply(logicalSource))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();

        if (matchedLogicalSourceResolverSuppliers.size() > 1) {
            LOG.debug(
                    "Found multiple matching resolvers [{}] for logical source {}",
                    matchedLogicalSourceResolverSuppliers.stream()
                            .map(Object::getClass)
                            .map(Class::getSimpleName),
                    log(logicalSource));
        }

        return MatchedLogicalSourceResolverFactory.select(matchedLogicalSourceResolverSuppliers)
                .orElseThrow(() -> new RmlMapperException(String.format(
                        "No logical source resolver supplier bound for reference formulation %s%nResolvers "
                                + "available: %s",
                        logicalSource.getReferenceFormulation(),
                        matchingLogicalSourceResolverFactories.stream()
                                .map(MatchingLogicalSourceResolverFactory::getResolverName)
                                .collect(joining(", ")))))
                .apply(logicalSource.getSource());
    }

    private RdfMapperConfig getEffectiveMapperConfig(TriplesMap triplesMap, RdfMapperConfig rdfMapperConfig) {
        IRI triplesMapBaseIri = triplesMap.getBaseIri();
        if (triplesMapBaseIri == null) {
            return rdfMapperConfig;
        }

        var overriddenTermGenConfig = rdfMapperConfig.getRdfTermGeneratorConfig().toBuilder()
                .baseIri(triplesMapBaseIri)
                .build();

        return rdfMapperConfig.toBuilder()
                .rdfTermGeneratorConfig(overriddenTermGenConfig)
                .build();
    }

    private LogicalSourceResolver<?> getTriplesMapLogicalSourceResolver(
            TriplesMap triplesMap, Map<Source, LogicalSourceResolver<?>> sourceToLogicalSourceResolver) {
        return sourceToLogicalSourceResolver.entrySet().stream()
                .filter(entry ->
                        entry.getKey().equals(requireLogicalSource(triplesMap).getSource()))
                .map(Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        String.format("LogicalSourceResolver not found for TriplesMap:%n%s", exception(triplesMap))));
    }

    private static LogicalSource requireLogicalSource(TriplesMap triplesMap) {
        if (triplesMap.getLogicalSource() instanceof LogicalSource logicalSource) {
            return logicalSource;
        }
        throw new RmlMapperException(String.format(
                "Expected LogicalSource but found %s for TriplesMap %s",
                triplesMap.getLogicalSource().getClass().getSimpleName(), triplesMap.getResourceName()));
    }
}
