package com.taxonic.carml.engine.rdf;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.LogicalSourcePipeline;
import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.engine.RmlMapperException;
import com.taxonic.carml.engine.TermGeneratorFactory;
import com.taxonic.carml.engine.function.Functions;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.MapDbChildSideJoinStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.MapDbParentSideJoinConditionStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStoreProvider;
import com.taxonic.carml.engine.sourceresolver.ClassPathResolver;
import com.taxonic.carml.engine.sourceresolver.CompositeSourceResolver;
import com.taxonic.carml.engine.sourceresolver.FileResolver;
import com.taxonic.carml.engine.sourceresolver.SourceResolver;
import com.taxonic.carml.engine.template.TemplateParser;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.Mapping;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

@NoArgsConstructor
public class RdfRmlMapperBuilder {
  private final Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers = new HashMap<>();

  // TODO validate triplesMaps?
  private Set<TriplesMap> triplesMaps;

  private Set<TriplesMap> mappableTriplesMaps;

  private final Functions functions = new Functions();

  private final Set<SourceResolver> sourceResolvers = new HashSet<>();

  private Supplier<ValueFactory> valueFactorySupplier = SimpleValueFactory::getInstance;

  private Normalizer.Form normalizationForm = Normalizer.Form.NFC;

  private boolean iriUpperCasePercentEncoding = true;

  private TermGeneratorFactory<Value> termGeneratorFactory;

  private ChildSideJoinStoreProvider<Resource, IRI> childSideJoinCacheProvider =
      MapDbChildSideJoinStoreProvider.getInstance();

  private ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider =
      MapDbParentSideJoinConditionStoreProvider.getInstance();

  public RdfRmlMapperBuilder addFunctions(Object... fn) {
    functions.addFunctions(fn);
    return this;
  }

  public RdfRmlMapperBuilder sourceResolver(SourceResolver sourceResolver) {
    sourceResolvers.add(sourceResolver);
    return this;
  }

  public RdfRmlMapperBuilder fileResolver(Path basePath) {
    sourceResolvers.add(FileResolver.of(basePath));
    return this;
  }

  public RdfRmlMapperBuilder classPathResolver(String basePath) {
    sourceResolvers.add(ClassPathResolver.of(basePath));
    return this;
  }

  public RdfRmlMapperBuilder setLogicalSourceResolver(IRI iri, Supplier<LogicalSourceResolver<?>> resolver) {
    logicalSourceResolverSuppliers.put(iri, resolver);
    return this;
  }

  // TODO is this necessary?
  public RdfRmlMapperBuilder removeLogicalSourceResolver(IRI iri) {
    logicalSourceResolverSuppliers.remove(iri);
    return this;
  }

  public RdfRmlMapperBuilder valueFactorySupplier(Supplier<ValueFactory> valueFactorySupplier) {
    this.valueFactorySupplier = valueFactorySupplier;
    return this;
  }

  public RdfRmlMapperBuilder iriUnicodeNormalization(Normalizer.Form normalizationForm) {
    this.normalizationForm = normalizationForm;
    return this;
  }

  /**
   * Builder option for backwards compatibility. RmlMapper used to percent encode IRIs with lower case
   * hex numbers. Now, the default is upper case hex numbers.
   *
   * @param iriUpperCasePercentEncoding true for upper case, false for lower case
   * @return {@link RmlMapper.Builder}
   */
  public RdfRmlMapperBuilder iriUpperCasePercentEncoding(boolean iriUpperCasePercentEncoding) {
    this.iriUpperCasePercentEncoding = iriUpperCasePercentEncoding;
    return this;
  }

  public RdfRmlMapperBuilder triplesMaps(Set<TriplesMap> triplesMaps) {
    this.triplesMaps = triplesMaps;
    this.mappableTriplesMaps = Mapping.filterMappable(triplesMaps);
    return this;
  }

  public RdfRmlMapperBuilder childSideJoinCacheProvider(
      ChildSideJoinStoreProvider<Resource, IRI> childSideJoinCacheProvider) {
    this.childSideJoinCacheProvider = childSideJoinCacheProvider;
    return this;
  }

  public RdfRmlMapperBuilder parentSideJoinConditionCacheProvider(
      ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider) {
    this.parentSideJoinConditionStoreProvider = parentSideJoinConditionStoreProvider;
    return this;
  }

  public RmlMapper<Statement> build() {
    if (logicalSourceResolverSuppliers.isEmpty()) {
      throw new RmlMapperException("No logical source resolver suppliers specified.");
    }

    if (termGeneratorFactory == null) {
      RdfMapperOptions mapperOptions = RdfMapperOptions.builder()
          .normalizationForm(normalizationForm)
          .iriUpperCasePercentEncoding(iriUpperCasePercentEncoding)
          .functions(functions)
          .build();

      termGeneratorFactory =
          RdfTermGeneratorFactory.of(valueFactorySupplier.get(), mapperOptions, TemplateParser.build());
    }

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(valueFactorySupplier)
        .termGeneratorFactory(termGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinCacheProvider)
        .build();

    Map<TriplesMap, Set<RdfRefObjectMapper>> tmToRoMappers = new HashMap<>();
    Map<RdfRefObjectMapper, TriplesMap> incomingRoMapperToParentTm = new HashMap<>();

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
            RdfRefObjectMapper roMapper =
                RdfRefObjectMapper.of(rom, triplesMap, rdfMappingContext, childSideJoinCacheProvider);
            roMappers.add(roMapper);
            incomingRoMapperToParentTm.put(roMapper, rom.getParentTriplesMap());
          });
      tmToRoMappers.put(triplesMap, roMappers);
    }

    Map<LogicalSource, List<TriplesMap>> groupedTriplesMaps = mappableTriplesMaps.stream()
        .collect(Collectors.groupingBy(TriplesMap::getLogicalSource));

    Set<RdfLogicalSourcePipeline<?>> logicalSourcePipelines = groupedTriplesMaps.entrySet()
        .stream()
        .map(triplesMapGroup -> buildRdfLogicalSourcePipeline(triplesMapGroup.getKey(), triplesMapGroup.getValue(),
            tmToRoMappers, incomingRoMapperToParentTm, rdfMappingContext))
        .collect(Collectors.toSet());

    Map<TriplesMap, LogicalSourcePipeline<?, Statement>> logicalSourcePipelinePool = new HashMap<>();
    for (RdfLogicalSourcePipeline<?> logicalSourcePipeline : logicalSourcePipelines) {
      logicalSourcePipeline.getTriplesMappers()
          .forEach(rdfTriplesMapper -> logicalSourcePipelinePool.put(rdfTriplesMapper.getTriplesMap(),
              logicalSourcePipeline));
    }

    CompositeSourceResolver compositeResolver = CompositeSourceResolver.of(ImmutableSet.copyOf(sourceResolvers));


    return new RmlMapper<>(compositeResolver, logicalSourcePipelinePool);
  }

  private RdfLogicalSourcePipeline<?> buildRdfLogicalSourcePipeline(LogicalSource logicalSource,
      List<TriplesMap> triplesMaps, Map<TriplesMap, Set<RdfRefObjectMapper>> tmToRoMappers,
      Map<RdfRefObjectMapper, TriplesMap> incomingRoMapperToParentTm, RdfMappingContext rdfMappingContext) {

    Supplier<LogicalSourceResolver<?>> logicalSourceResolverSupplier =
        logicalSourceResolverSuppliers.get(logicalSource.getReferenceFormulation());

    if (logicalSourceResolverSuppliers == null) {
      throw new RmlMapperException(
          String.format("No logical source resolver supplier bound for reference formulation %s",
              logicalSource.getReferenceFormulation()));
    }

    return RdfLogicalSourcePipeline.of(logicalSource, triplesMaps, tmToRoMappers, incomingRoMapperToParentTm,
        logicalSourceResolverSupplier.get(), rdfMappingContext, parentSideJoinConditionStoreProvider);
  }

}
