package io.carml.engine.rdf;

import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.engine.MappingPipeline;
import io.carml.engine.RmlMapper;
import io.carml.engine.RmlMapperException;
import io.carml.engine.TermGeneratorFactory;
import io.carml.engine.function.Functions;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import io.carml.engine.join.ParentSideJoinConditionStoreProvider;
import io.carml.engine.join.impl.CarmlChildSideJoinStoreProvider;
import io.carml.engine.join.impl.CarmlParentSideJoinConditionStoreProvider;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverSupplier;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sourceresolver.CompositeSourceResolver;
import io.carml.logicalsourceresolver.sourceresolver.FileResolver;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolver;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseConnectionOptions;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseSourceResolver;
import io.carml.model.TriplesMap;
import io.carml.util.Mappings;
import java.io.InputStream;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
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

  private static final IRI RML_BASE_IRI = iri("http://example.com/base/");

  private static final long SECONDS_TO_TIMEOUT = 30;

  private RdfRmlMapper(Set<TriplesMap> triplesMaps, Function<Object, Optional<Object>> sourceResolver,
      MappingPipeline<Statement> mappingPipeline) {
    super(triplesMaps, sourceResolver, mappingPipeline);
  }

  public static Builder builder() {
    return new Builder();
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Builder {

    private IRI baseIri = RML_BASE_IRI;

    private final Map<IRI, Supplier<LogicalSourceResolver<?>>> logicalSourceResolverSuppliers = new HashMap<>();

    private final Set<MatchingLogicalSourceResolverSupplier> matchingLogicalSourceResolverSuppliers =
        new LinkedHashSet<>();

    private Set<TriplesMap> triplesMaps = new HashSet<>();

    private final Functions functions = new Functions();

    private final Set<SourceResolver> sourceResolvers = new LinkedHashSet<>();

    private Supplier<ValueFactory> valueFactorySupplier = SimpleValueFactory::getInstance;

    private Normalizer.Form normalizationForm = Normalizer.Form.NFC;

    private boolean iriUpperCasePercentEncoding = true;

    private TermGeneratorFactory<Value> termGeneratorFactory;

    private ChildSideJoinStoreProvider<Resource, IRI> childSideJoinCacheProvider = CarmlChildSideJoinStoreProvider.of();

    private ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider =
        CarmlParentSideJoinConditionStoreProvider.of();

    private DatabaseConnectionOptions databaseConnectionOptions;

    /**
     * Sets the base IRI used in resolving relative IRIs produced by RML mappings.<br>
     * If not set, the base IRI will default to <code>"http://example.com/base/"</code>.
     *
     * @param baseIriString the base IRI String
     * @return {@link Builder}
     */
    public Builder baseIri(String baseIriString) {
      return baseIri(iri(baseIriString));
    }

    /**
     * Sets the base IRI used in resolving relative IRIs produced by RML mappings.<br>
     * If not set, the base IRI will default to <code>&lt;http://example.com/base/&gt;</code>.
     *
     * @param baseIri the base IRI
     * @return {@link Builder}
     */
    public Builder baseIri(IRI baseIri) {
      this.baseIri = baseIri;
      return this;
    }

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

    // TODO: deprecate
    public Builder setLogicalSourceResolver(IRI iri, Supplier<LogicalSourceResolver<?>> resolverSupplier) {
      logicalSourceResolverSuppliers.put(iri, resolverSupplier);
      return this;
    }

    public Builder logicalSourceResolverMatcher(MatchingLogicalSourceResolverSupplier matchingResolverSupplier) {
      matchingLogicalSourceResolverSuppliers.add(matchingResolverSupplier);
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

    // Will override all connections
    public Builder databaseConnectionOptions(DatabaseConnectionOptions databaseConnectionOptions) {
      this.databaseConnectionOptions = databaseConnectionOptions;
      return this;
    }

    public RdfRmlMapper build() {
      if (matchingLogicalSourceResolverSuppliers.isEmpty() && logicalSourceResolverSuppliers.isEmpty()) {
        throw new RmlMapperException("No logical source resolver suppliers specified.");
      }

      RdfTermGeneratorConfig rdfTermGeneratorConfig = RdfTermGeneratorConfig.builder()
          .baseIri(baseIri)
          .valueFactory(valueFactorySupplier.get())
          .normalizationForm(normalizationForm)
          .iriUpperCasePercentEncoding(iriUpperCasePercentEncoding)
          .functions(functions)
          .build();

      if (termGeneratorFactory == null) {
        termGeneratorFactory = RdfTermGeneratorFactory.of(rdfTermGeneratorConfig);
      }

      if (databaseConnectionOptions != null) {
        sourceResolvers.add(DatabaseSourceResolver.of(databaseConnectionOptions));
      } else {
        sourceResolvers.add(DatabaseSourceResolver.of());
      }

      if (sourceResolvers.stream()
          .noneMatch(FileResolver.class::isInstance)) {
        // Add default file resolver
        sourceResolvers.add(FileResolver.of());
      }

      var compositeResolver = CompositeSourceResolver.of(sourceResolvers);

      System.getProperties()
          .setProperty("org.jooq.no-logo", "true");

      System.getProperties()
          .setProperty("org.jooq.no-tips", "true");

      var rdfMapperConfig = RdfMapperConfig.builder()
          .valueFactorySupplier(valueFactorySupplier)
          .termGeneratorFactory(termGeneratorFactory)
          .childSideJoinStoreProvider(childSideJoinCacheProvider)
          .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
          .build();

      var pipelineFactory = RdfMappingPipelineFactory.getInstance();

      var mappableTriplesMaps = Mappings.filterMappable(triplesMaps);

      var mappingPipeline = pipelineFactory.getMappingPipeline(mappableTriplesMaps, rdfMapperConfig,
          logicalSourceResolverSuppliers, matchingLogicalSourceResolverSuppliers);

      return new RdfRmlMapper(triplesMaps, compositeResolver, mappingPipeline);
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
