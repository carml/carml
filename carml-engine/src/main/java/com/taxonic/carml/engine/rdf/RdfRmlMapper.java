package com.taxonic.carml.engine.rdf;

import static com.taxonic.carml.util.LogUtil.exception;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import com.taxonic.carml.engine.RefObjectMapper;
import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.engine.RmlMapperException;
import com.taxonic.carml.engine.TermGeneratorFactory;
import com.taxonic.carml.engine.TriplesMapper;
import com.taxonic.carml.engine.function.Functions;
import com.taxonic.carml.engine.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.engine.join.ParentSideJoinConditionStoreProvider;
import com.taxonic.carml.engine.join.impl.CarmlChildSideJoinStoreProvider;
import com.taxonic.carml.engine.join.impl.CarmlParentSideJoinConditionStoreProvider;
import com.taxonic.carml.engine.sourceresolver.ClassPathResolver;
import com.taxonic.carml.engine.sourceresolver.CompositeSourceResolver;
import com.taxonic.carml.engine.sourceresolver.FileResolver;
import com.taxonic.carml.engine.sourceresolver.SourceResolver;
import com.taxonic.carml.engine.template.TemplateParser;
import com.taxonic.carml.logicalsourceresolver.LogicalSourceResolver;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.Mapping;
import java.io.InputStream;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import reactor.core.publisher.Flux;

@Slf4j
public class RdfRmlMapper extends RmlMapper<Statement> {

  private static final long SECONDS_TO_TIMEOUT = 30;

  private RdfRmlMapper(Set<TriplesMap> triplesMaps, Function<Object, Optional<Object>> sourceResolver,
      Set<TriplesMapper<Statement>> triplesMappers,
      Map<RefObjectMapper<Statement>, TriplesMapper<Statement>> refObjectMapperToParentTriplesMapper,
      Map<Object, LogicalSourceResolver<?>> sourceToLogicalSourceResolver) {
    super(triplesMaps, sourceResolver, triplesMappers, refObjectMapperToParentTriplesMapper,
        sourceToLogicalSourceResolver);
  }

  public static Builder builder() {
    return new Builder();
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Builder {
    private final Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers = new HashMap<>();

    private Set<TriplesMap> triplesMaps = new HashSet<>();

    private Set<TriplesMap> mappableTriplesMaps = new HashSet<>();

    private final Functions functions = new Functions();

    private final Set<SourceResolver> sourceResolvers = new HashSet<>();

    private Supplier<ValueFactory> valueFactorySupplier = SimpleValueFactory::getInstance;

    private Normalizer.Form normalizationForm = Normalizer.Form.NFC;

    private boolean iriUpperCasePercentEncoding = true;

    private TermGeneratorFactory<Value> termGeneratorFactory;

    private ChildSideJoinStoreProvider<Resource, IRI> childSideJoinCacheProvider = CarmlChildSideJoinStoreProvider.of();

    private ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider =
        CarmlParentSideJoinConditionStoreProvider.of();

    public Builder addFunctions(Object... fn) {
      functions.addFunctions(fn);
      return this;
    }

    public Builder sourceResolver(SourceResolver sourceResolver) {
      sourceResolvers.add(sourceResolver);
      return this;
    }

    public Builder fileResolver(Path basePath) {
      sourceResolvers.add(FileResolver.of(basePath));
      return this;
    }

    public Builder classPathResolver(String basePath) {
      sourceResolvers.add(ClassPathResolver.of(basePath));
      return this;
    }

    public Builder classPathResolver(ClassPathResolver classPathResolver) {
      sourceResolvers.add(classPathResolver);
      return this;
    }

    public Builder setLogicalSourceResolver(IRI iri, Supplier<LogicalSourceResolver<?>> resolverSupplier) {
      logicalSourceResolverSuppliers.put(iri, resolverSupplier);
      return this;
    }

    public Builder valueFactorySupplier(Supplier<ValueFactory> valueFactorySupplier) {
      this.valueFactorySupplier = valueFactorySupplier;
      return this;
    }

    public Builder iriUnicodeNormalization(Normalizer.Form normalizationForm) {
      this.normalizationForm = normalizationForm;
      return this;
    }

    /**
     * Builder option for backwards compatibility. RmlMapper used to percent encode IRIs with lower case
     * hex numbers. Now, the default is upper case hex numbers.
     *
     * @param iriUpperCasePercentEncoding true for upper case, false for lower case
     * @return {@link Builder}
     */
    public Builder iriUpperCasePercentEncoding(boolean iriUpperCasePercentEncoding) {
      this.iriUpperCasePercentEncoding = iriUpperCasePercentEncoding;
      return this;
    }

    public Builder triplesMaps(Set<TriplesMap> triplesMaps) {
      this.triplesMaps = triplesMaps;
      this.mappableTriplesMaps = Mapping.filterMappable(triplesMaps);
      return this;
    }

    public Builder childSideJoinStoreProvider(ChildSideJoinStoreProvider<Resource, IRI> childSideJoinCacheProvider) {
      this.childSideJoinCacheProvider = childSideJoinCacheProvider;
      return this;
    }

    public Builder parentSideJoinConditionStoreProvider(
        ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider) {
      this.parentSideJoinConditionStoreProvider = parentSideJoinConditionStoreProvider;
      return this;
    }

    public RdfRmlMapper build() {
      if (logicalSourceResolverSuppliers.isEmpty()) {
        throw new RmlMapperException("No logical source resolver suppliers specified.");
      }

      RdfMapperOptions mapperOptions = RdfMapperOptions.builder()
          .valueFactory(valueFactorySupplier.get())
          .normalizationForm(normalizationForm)
          .iriUpperCasePercentEncoding(iriUpperCasePercentEncoding)
          .functions(functions)
          .build();

      if (termGeneratorFactory == null) {
        termGeneratorFactory =
            RdfTermGeneratorFactory.of(mapperOptions, TemplateParser.build(), parentSideJoinConditionStoreProvider);
      }

      var rdfMappingContext = RdfMappingContext.builder()
          .valueFactorySupplier(valueFactorySupplier)
          .termGeneratorFactory(termGeneratorFactory)
          .childSideJoinStoreProvider(childSideJoinCacheProvider)
          .build();

      Map<TriplesMap, Set<RdfRefObjectMapper>> tmToRoMappers = new HashMap<>();
      Map<RdfRefObjectMapper, TriplesMap> roMapperToParentTm = new HashMap<>();

      if (mappableTriplesMaps.isEmpty()) {
        throw new RmlMapperException("No actionable triples maps provided.");
      }

      for (TriplesMap triplesMap : mappableTriplesMaps) {
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
              var roMapper = RdfRefObjectMapper.of(rom, triplesMap, rdfMappingContext, childSideJoinCacheProvider);
              roMappers.add(roMapper);
              roMapperToParentTm.put(roMapper, rom.getParentTriplesMap());
            });
        tmToRoMappers.put(triplesMap, roMappers);
      }

      var parentTmToRoMappers = roMapperToParentTm.entrySet()
          .stream()
          .collect(groupingBy(Map.Entry::getValue, mapping(Map.Entry::getKey, toSet())));

      var sourceToLogicalSourceResolver = buildLogicalSourceResolvers(mappableTriplesMaps);

      Set<TriplesMapper<Statement>> triplesMappers = mappableTriplesMaps.stream()
          .map(triplesMap -> RdfTriplesMapper.of(triplesMap, tmToRoMappers.get(triplesMap),
              !parentTmToRoMappers.containsKey(triplesMap) ? Set.of() : parentTmToRoMappers.get(triplesMap),
              getExpressionEvaluationFactory(triplesMap, sourceToLogicalSourceResolver), rdfMappingContext,
              parentSideJoinConditionStoreProvider))
          .collect(Collectors.toUnmodifiableSet());

      Map<RefObjectMapper<Statement>, TriplesMapper<Statement>> roMapperToParentTriplesMapper =
          roMapperToParentTm.entrySet()
              .stream()
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                  entry -> getTriplesMapper(entry.getValue(), triplesMappers)));

      var compositeResolver = CompositeSourceResolver.of(Set.copyOf(sourceResolvers));

      return new RdfRmlMapper(triplesMaps, compositeResolver, triplesMappers, roMapperToParentTriplesMapper,
          sourceToLogicalSourceResolver);
    }

    private Map<Object, LogicalSourceResolver<?>> buildLogicalSourceResolvers(Set<TriplesMap> triplesMaps) {

      if (triplesMaps.isEmpty()) {
        throw new RmlMapperException("No executable triples maps found.");
      }

      var sourceToLogicalSources = triplesMaps.stream()
          .map(TriplesMap::getLogicalSource)
          .collect(groupingBy(LogicalSource::getSource, toSet()));

      return sourceToLogicalSources.entrySet()
          .stream()
          .collect(
              Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> buildLogicalSourceResolver(entry.getValue())));
    }

    private LogicalSourceResolver<?> buildLogicalSourceResolver(Set<LogicalSource> logicalSources) {
      var referenceFormulation = logicalSources.stream()
          .map(LogicalSource::getReferenceFormulation)
          .findFirst();

      return referenceFormulation.map(this::getLogicalSourceResolver)
          .orElseThrow(() -> new RmlMapperException(
              String.format("No logical sources found in triplesMaps:%n%s", exception(triplesMaps))));
    }

    private LogicalSourceResolver<?> getLogicalSourceResolver(IRI referenceFormulation) {
      var logicalSourceResolverSupplier = logicalSourceResolverSuppliers.get(referenceFormulation);

      if (logicalSourceResolverSupplier == null) {
        throw new RmlMapperException(String
            .format("No logical source resolver supplier bound for reference formulation %s", referenceFormulation));
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

  public Model mapToModel() {
    return toModel(map());
  }

  public Model mapToModel(Set<TriplesMap> triplesMapFilter) {
    return toModel(map(triplesMapFilter));
  }

  public Model mapToModel(@NonNull InputStream inputStream) {
    return toModel(map(inputStream));
  }

  public Model mapToModel(@NonNull InputStream inputStream, Set<TriplesMap> triplesMapFilter) {
    return toModel(map(inputStream, triplesMapFilter));
  }

  public Model mapToModel(Map<String, InputStream> namedInputStreams) {
    return toModel(map(namedInputStreams));
  }

  public Model mapToModel(Map<String, InputStream> namedInputStreams, Set<TriplesMap> triplesMapFilter) {
    return toModel(map(namedInputStreams, triplesMapFilter));
  }

  public <R> Model mapRecordToModel(R providedRecord, Class<R> providedRecordClass) {
    return toModel(mapRecord(providedRecord, providedRecordClass));
  }

  public <R> Model mapRecordToModel(R providedRecord, Class<R> providedRecordClass, Set<TriplesMap> triplesMapFilter) {
    return toModel(mapRecord(providedRecord, providedRecordClass, triplesMapFilter));
  }

  private Model toModel(Flux<Statement> statementFlux) {
    return statementFlux.collect(ModelCollector.toModel())
        .block(Duration.ofSeconds(SECONDS_TO_TIMEOUT));
  }
}
