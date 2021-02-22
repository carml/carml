package com.taxonic.carml.engine;

import com.taxonic.carml.engine.function.Functions;
import com.taxonic.carml.engine.source_resolver.CarmlStreamResolver;
import com.taxonic.carml.engine.source_resolver.ClassPathResolver;
import com.taxonic.carml.engine.source_resolver.CompositeSourceResolver;
import com.taxonic.carml.engine.source_resolver.FileResolver;
import com.taxonic.carml.engine.source_resolver.SourceResolver;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.CreateContextEvaluate;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.CreateSimpleTypedRepresentation;
import com.taxonic.carml.model.BaseObjectMap;
import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.ContextSource;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.MergeSuper;
import com.taxonic.carml.model.NestedMapping;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TermMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import com.taxonic.carml.util.ModelSerializer;
import com.taxonic.carml.util.RmlNamespaces;
import com.taxonic.carml.vocab.Rdf;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Path;
import java.text.Normalizer.Form;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

// TODO cache results of evaluated expressions when filling a single template, in case of repeated expressions

// TODO template strings should be validated during the validation step?

/* TODO re-use the ***Mapper instances for equal corresponding ***Map instances.
 * f.e. if there are 2 equal PredicateMaps in the RML mapping file,
 * re-use the same PredicateMapper instance
 */

// TODO break this class up into smaller parts

@SuppressWarnings({ "squid:S112", "squid:S3864" })
public class RmlMapper {

	private static final Logger LOG = LoggerFactory.getLogger(RmlMapper.class);

	public static final String DEFAULT_STREAM_NAME = "DEFAULT";

	private final LogicalSourceManager sourceManager;

	private final LogicalSourceAspect logicalSourceAspect;

	private final TermGeneratorCreator termGenerators;

	private RmlMapper(
		LogicalSourceAspect logicalSourceAspect,
		TermGeneratorCreator termGenerators
	) {
		this.logicalSourceAspect = logicalSourceAspect;
		this.termGenerators = termGenerators; // TODO rename
		this.sourceManager = new LogicalSourceManager();
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {

		private final Functions functions = new Functions();
		private final Set<SourceResolver> sourceResolvers = new HashSet<>();
		private final Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers = new HashMap<>();
		private Form normalizationForm = Form.NFC;
		private boolean iriUpperCasePercentEncoding = true;

		public Builder addFunctions(Object... fn) {
			functions.addFunctions(fn);
			return this;
		}

		public Builder sourceResolver(SourceResolver sourceResolver) {
			sourceResolvers.add(sourceResolver);
			return this;
		}

		public Builder fileResolver(Path basePath) {
			sourceResolvers.add(new FileResolver(basePath));
			return this;
		}

		public Builder setLogicalSourceResolver(IRI iri, LogicalSourceResolver<?> resolver) {
			logicalSourceResolvers.put(iri, resolver);
			return this;
		}

		public Builder removeLogicalSourceResolver(IRI iri) {
			logicalSourceResolvers.remove(iri);
			return this;
		}

		public Builder classPathResolver(String basePath) {
			sourceResolvers.add(new ClassPathResolver(basePath));
			return this;
		}

		public Builder iriUnicodeNormalization(Form normalizationForm) {
			this.normalizationForm = normalizationForm;
			return this;
		}

		/**
		 * Builder option for backwards compatibility. RmlMapper used to percent encode
		 * IRIs with lower case hex numbers. Now, the default is upper case hex numbers.
		 * 
		 * @param iriUpperCasePercentEncoding true for upper case, false for lower case
		 * @return {@link Builder}
		 */
		public Builder iriUpperCasePercentEncoding(boolean iriUpperCasePercentEncoding) {
			this.iriUpperCasePercentEncoding = iriUpperCasePercentEncoding;
			return this;
		}

		public RmlMapper build() {

			CarmlStreamResolver carmlStreamResolver = new CarmlStreamResolver();

			if (logicalSourceResolvers.isEmpty()) {
				LOG.warn("No Logical Source Resolvers set.");
			}

			CompositeSourceResolver compositeResolver =
					new CompositeSourceResolver(
						// prepend carml stream resolver to regular resolvers
						Stream.concat(
							Stream.of(carmlStreamResolver),
							sourceResolvers.stream()
						)
						.collect(ImmutableCollectors.toImmutableSet())
					);

			TermGeneratorCreator termGeneratorCreator = TermGeneratorCreator
				.create(normalizationForm, functions, iriUpperCasePercentEncoding);
			RmlMapper mapper =
				new RmlMapper(
					new LogicalSourceAspect(compositeResolver, logicalSourceResolvers),
					termGeneratorCreator
				);
			termGeneratorCreator.setCreateSubjectMapper(mapper::createSubjectMapper);

			// Resolvers need a reference to the source manager, to manage
			// the caching of sources.
			compositeResolver.setSourceManager(mapper.getSourceManager());

			return mapper;
		}
	}

	public LogicalSourceManager getSourceManager() {
		return sourceManager;
	}

	public Model map(Set<TriplesMap> mapping) {
		validateMapping(mapping);
		Model model = new LinkedHashModel();

		Set<TriplesMap> functionValueTriplesMaps = getTermMaps(mapping)
				.map(TermMap::getFunctionValue)
				.filter(Objects::nonNull)
				.collect(ImmutableCollectors.toImmutableSet());

		if (LOG.isWarnEnabled()) {
			boolean deprecatedFno = functionValueTriplesMaps.stream()
					.flatMap(triplesMap -> triplesMap.getPredicateObjectMaps().stream())
					.flatMap(pom -> pom.getPredicateMaps().stream())
					.anyMatch(predicateMap -> predicateMap.getConstant().equals(Rdf.Fno.old_executes));
			if (deprecatedFno) {
				LOG.warn("Usage of deprecated predicate <{}> encountered. Support in next release is not guaranteed. Upgrade to <{}>.",
						Rdf.Fno.old_executes, Rdf.Fno.executes);
			}
		}

		Set<TriplesMap> refObjectTriplesMaps = getAllTriplesMapsUsedInRefObjectMap(mapping);

		mapping.stream()
			.filter(m -> !functionValueTriplesMaps.contains(m) ||
				refObjectTriplesMaps.contains(m))
			.forEach(m -> map(m, model));
		sourceManager.clear();
		return model;
	}

	private void validateMapping(Set<TriplesMap> mapping) {
		Objects.requireNonNull(mapping);
		if (mapping.isEmpty()) {
			throw new RuntimeException("Empty mapping provided. Please make sure your mapping is syntactically correct.");
		}
	}

	private Stream<TermMap> getTermMaps(Set<TriplesMap> mapping) {
		// TODO add nested maps here as well
		return mapping.stream()
				.flatMap(m ->
					Stream.concat (
						m.getPredicateObjectMaps()
							.stream()
							.flatMap(p ->
								Stream.concat(
 										p.getGraphMaps().stream(),
									Stream.concat(
										p.getPredicateMaps().stream(),
										p.getObjectMaps().stream()
											.filter(ObjectMap.class::isInstance)
											.map(ObjectMap.class::cast)
									)
								)
							),
						Stream.concat(
							Stream.of(m.getSubjectMap()),
							m.getSubjectMap() != null ?
									m.getSubjectMap().getGraphMaps().stream() :
									Stream.empty()
						)
					)
				)
				.filter(Objects::nonNull);
	}

	private Set<TriplesMap> getAllTriplesMapsUsedInRefObjectMap(Set<TriplesMap> mapping) {
		// TODO do i have to add something here re nested mappings?
		return mapping.stream()
				// get all referencing object maps
				.flatMap(m -> m.getPredicateObjectMaps().stream())
				.flatMap(p -> p.getObjectMaps().stream())
				.filter(o -> o instanceof RefObjectMap)
				.map(o -> (RefObjectMap) o)

				// check that no referencing object map
				// has 'map' as its parent triples map
				.map(RefObjectMap::getParentTriplesMap)
				.collect(ImmutableCollectors.toImmutableSet());
	}

	static String log(com.taxonic.carml.model.Resource ancestor, com.taxonic.carml.model.Resource resource) {
		return ModelSerializer.formatResourceForLog(ancestor.asRdf(), resource.getAsResource(),
				RmlNamespaces.RML_NAMESPACES, false);
	}

	static String exception(com.taxonic.carml.model.Resource ancestor, com.taxonic.carml.model.Resource resource) {
		return ModelSerializer.formatResourceForLog(ancestor.asRdf(), resource.getAsResource(),
				RmlNamespaces.RML_NAMESPACES, true);
	}

	private void map(TriplesMap triplesMap, Model model) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Mapping triples map: {}", triplesMap.getResourceName());
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("{}", log(triplesMap, triplesMap));
		}
		Consumer<Model> triplesMapper = createTriplesMapper(triplesMap); // TODO cache mapper instances
		triplesMapper.accept(model);
	}

	private Set<TermGenerator<IRI>> createGraphGenerators(Set<GraphMap> graphMaps, UnaryOperator<Object> createSimpleTypedRepresentation) {
		return graphMaps.stream()
			.map(g -> termGenerators.getGraphGenerator(g, createSimpleTypedRepresentation))
			.collect(ImmutableCollectors.toImmutableSet());
	}

	private Stream<TermGenerator<Value>> getObjectMapGenerators(
		Set<BaseObjectMap> objectMaps,
		TriplesMap triplesMap,
		UnaryOperator<Object> createSimpleTypedRepresentation
	) {
		return objectMaps.stream()
			.filter(o -> o instanceof ObjectMap)
			.peek(o -> LOG.debug("Creating term generator for ObjectMap {}", o.getResourceName()))
			.map(o -> {
				try {
					return termGenerators.getObjectGenerator((ObjectMap) o, createSimpleTypedRepresentation);
				} catch (RuntimeException ex) {
					throw new RuntimeException(String.format("Exception occurred for %s", exception(triplesMap, o)), ex);
				}
			});
	}

	private NestedMapping checkSuperLogicalSource(NestedMapping nestedMapping, TriplesMap triplesMap) {

		TriplesMap nestedTriplesMap = nestedMapping.getTriplesMap();
		LogicalSource nestedLogicalSource = nestedTriplesMap.getLogicalSource();

		if (nestedLogicalSource.getSource() instanceof ContextSource) {
			return nestedMapping;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Checking if super logicalSource for nested triples map {} is equal",
				nestedTriplesMap.getResourceName());
		}
		LogicalSource superLogicalSource = nestedLogicalSource.getMergeSuper() == null
			? null : nestedLogicalSource.getMergeSuper().getLogicalSource();
		if (!triplesMap.getLogicalSource().equals(superLogicalSource)) {
			throw new RuntimeException(String.format(
					"Logical sources are not equal.%n%nNested super logical source: %s%n%nContaining triples map logical source: %s%n%nNot equal in NestedMapping %s",
					log(nestedTriplesMap, nestedLogicalSource), log(triplesMap, triplesMap.getLogicalSource()),
					exception(triplesMap, nestedMapping)));
		}
		return nestedMapping;
	}

	private RefObjectMap checkLogicalSource(RefObjectMap refObjectMap, LogicalSource logicalSource, TriplesMap triplesMap) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Checking if logicalSource for parent triples map {} is equal",
					refObjectMap.getParentTriplesMap().getResourceName());
		}
		LogicalSource parentLogicalSource = refObjectMap.getParentTriplesMap().getLogicalSource();
		if (!logicalSource.equals(parentLogicalSource)) {
			throw new RuntimeException(String.format(
					"Logical sources are not equal.%n%nParent logical source: %s%n%nChild logical source: %s%n%nNot equal in RefObjectMap %s",
					log(refObjectMap.getParentTriplesMap(), parentLogicalSource), log(triplesMap, logicalSource),
					exception(triplesMap, refObjectMap)));
		}
		return refObjectMap;
	}

	private Stream<TermGenerator<? extends Value>> getJoinlessRefObjectMapGenerators(
		Set<BaseObjectMap> objectMaps, TriplesMap triplesMap
	) {

		LogicalSource logicalSource = triplesMap.getLogicalSource();

		return objectMaps.stream()
			.filter(objectMap -> objectMap instanceof RefObjectMap)
			.peek(objectMap -> LOG.debug("Creating mapper for RefObjectMap {}", objectMap.getResourceName()))
			.map(objectMap -> (RefObjectMap) objectMap)
			.filter(refObjMap -> refObjMap.getJoinConditions().isEmpty())
			.map(refObjMap -> checkLogicalSource(refObjMap, logicalSource, triplesMap))
			.map(refObjMap -> createRefObjectJoinlessMapper(refObjMap, triplesMap));
	}

	private TermGenerator<Resource> createRefObjectJoinlessMapper(
		RefObjectMap refObjectMap,
		TriplesMap triplesMap
	) {
		try {
			TriplesMap parentTriplesMap = refObjectMap.getParentTriplesMap();
			return termGenerators.getSubjectGenerator(
					parentTriplesMap.getSubjectMap(),
					getCreateSimpleTypedRepresentation(parentTriplesMap)
			);
		} catch (RuntimeException ex) {
			throw new RuntimeException(String.format("Exception occurred for %s", exception(triplesMap, refObjectMap)), ex);
		}
	}

	private Set<PredicateObjectMapper> createPredicateObjectMappers(
		TriplesMap triplesMap,
		Set<PredicateObjectMap> predicateObjectMaps,
		CreateSimpleTypedRepresentation createSimpleTypedRepresentation
	) {
		return predicateObjectMaps.stream()
				.peek(m -> LOG.debug("Creating mapper for PredicateObjectMap {}", m.getResourceName()))
				.map(m -> {
					Set<BaseObjectMap> objectMaps = m.getObjectMaps();

					Set<PredicateMapper> predicateMappers =
						m.getPredicateMaps().stream()
							.map(p -> createPredicateMapper(p, objectMaps, triplesMap, createSimpleTypedRepresentation))
							.collect(ImmutableCollectors.toImmutableSet());

					return new PredicateObjectMapper(
						createGraphGenerators(m.getGraphMaps(), createSimpleTypedRepresentation),
						predicateMappers
					);
				})
				.collect(ImmutableCollectors.toImmutableSet());
	}

	PredicateMapper createPredicateMapper(
		PredicateMap predicateMap,
		Set<BaseObjectMap> objectMaps,
		TriplesMap triplesMap,
		CreateSimpleTypedRepresentation createSimpleTypedRepresentation
	) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating mapper for PredicateMap {}", log(triplesMap, predicateMap));
		}

		Set<TermGenerator<? extends Value>> objectGenerators =
			Stream.concat(

				// object maps -> object generators
				getObjectMapGenerators(objectMaps, triplesMap, createSimpleTypedRepresentation),

				// ref object maps without joins -> object generators.
				// ref object maps without joins MUST have an identical logical source.
				getJoinlessRefObjectMapGenerators(objectMaps, triplesMap)

			)
			.collect(ImmutableCollectors.toImmutableSet());

		Set<RefObjectMapper> refObjectMappers =
			objectMaps.stream()
				.filter(o -> o instanceof RefObjectMap)
				.map(o -> (RefObjectMap) o)
				.filter(o -> !o.getJoinConditions().isEmpty())
				.map(r -> createRefObjectMapper(r, createSimpleTypedRepresentation))
				.collect(ImmutableCollectors.toImmutableSet());

		Set<NestedMapper<?>> nestedMappers = objectMaps.stream()
			.filter(o -> o instanceof NestedMapping)
			.map(o -> (NestedMapping) o)
			.map(n -> checkSuperLogicalSource(n, triplesMap))
			.map(this::createNestedMapper)
			.collect(ImmutableCollectors.toImmutableSet());

		TermGenerator<IRI> predicateGenerator;
		try{
			predicateGenerator = termGenerators.getPredicateGenerator(predicateMap, createSimpleTypedRepresentation);
		} catch(RuntimeException ex) {
			throw new RuntimeException(String.format("Exception occurred for %s", exception(triplesMap, predicateMap)), ex);
		}

		return new PredicateMapper(
			predicateGenerator,
			objectGenerators,
			refObjectMappers,
			nestedMappers
		);
	}

	SubjectMapper createSubjectMapper(TriplesMap triplesMap) {
		SubjectMap subjectMap = triplesMap.getSubjectMap();
		if (subjectMap == null) {
			throw new RuntimeException(
					String.format("Subject map must be specified in triples map %s",
							exception(triplesMap, triplesMap)));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating mapper for SubjectMap {}", log(triplesMap, subjectMap));
		}

		CreateSimpleTypedRepresentation createSimpleTypedRepresentation = getCreateSimpleTypedRepresentation(triplesMap);

		TermGenerator<Resource> subjectGenerator;
		try {
			subjectGenerator = termGenerators.getSubjectGenerator(subjectMap, createSimpleTypedRepresentation);
		} catch(RuntimeException ex) {
			throw new RuntimeException(String.format("Exception occurred for %s", exception(triplesMap, subjectMap)), ex);
		}

		return new SubjectMapper(
			subjectGenerator,
			createGraphGenerators(subjectMap.getGraphMaps(), createSimpleTypedRepresentation),
			subjectMap.getClasses(),
			createPredicateObjectMappers(triplesMap, triplesMap.getPredicateObjectMaps(), createSimpleTypedRepresentation)
		);
	}

	private Consumer<Model> createTriplesMapper(TriplesMap triplesMap) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating mapper for TriplesMap {}", triplesMap.getResourceName());
		}

		TriplesMapper<Object> triplesMapper = new TriplesMapper<>(
			triplesMap.getResourceName(),
			createGetStream(triplesMap),
			createSubjectMapper(triplesMap),
			createNestedMappers(triplesMap)
		);

		return triplesMapper::map;
	}

	Supplier<Stream<Item<Object>>> createGetStream(TriplesMap triplesMap) {
		return logicalSourceAspect.createGetStream(triplesMap);
	}

	private <T> Set<NestedMapper<T>> createNestedMappers(TriplesMap triplesMap) {
		return triplesMap.getNestedMappings().stream()
			.map(this::<T>createNestedMapper).collect(Collectors.toSet());
	}

	private <T> NestedMapper<T> createNestedMapper(NestedMapping nestedMapping) {

		TriplesMap triplesMap = nestedMapping.getTriplesMap();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating (context) mapper for TriplesMap {}", triplesMap.getResourceName());
		}

		LogicalSourceResolver<T> logicalSourceResolver = logicalSourceAspect.getLogicalSourceResolver(triplesMap);
		LogicalSource logicalSource = triplesMap.getLogicalSource();

		ContextTriplesMapper<T> triplesMapper = new ContextTriplesMapper<>(
			triplesMap.getResourceName(),
			logicalSourceResolver.createGetStreamFromContext(logicalSource.getIterator()),
			createSubjectMapper(triplesMap),
			createNestedMappers(triplesMap)
		);

		CreateContextEvaluate createContextEvaluate = logicalSourceResolver.getCreateContextEvaluate();

		return new NestedMapper<>(triplesMapper,
			getContextEntriesFromNestedDeclarationOrSuperLogicalSource(nestedMapping, logicalSource), createContextEvaluate);
	}

	private Set<ContextEntry> getContextEntriesFromNestedDeclarationOrSuperLogicalSource(NestedMapping nestedMapping, LogicalSource logicalSource) {
		Set<ContextEntry> contextEntries = nestedMapping.getContextEntries();
		if (!contextEntries.isEmpty()) {
			return contextEntries;
		}
		return Optional.ofNullable(logicalSource.getMergeSuper())
			.map(MergeSuper::getIncluding)
			.orElse(Collections.emptySet());
	}

	private ParentTriplesMapper<?> createParentTriplesMapper(
		TriplesMap triplesMap
	) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating mapper for ParentTriplesMap {}", triplesMap.getResourceName());
		}

		try {
			CreateSimpleTypedRepresentation createSimpleTypedRepresentation = getCreateSimpleTypedRepresentation(triplesMap);
			return new ParentTriplesMapper<>(
				termGenerators.getSubjectGenerator(triplesMap.getSubjectMap(), createSimpleTypedRepresentation),
				createGetStream(triplesMap),
				createSimpleTypedRepresentation
			);
		}
		catch(RuntimeException ex) {
			throw new RuntimeException(String.format("Exception occurred for %s", exception(triplesMap, triplesMap.getSubjectMap())), ex);
		}
	}

	private CreateSimpleTypedRepresentation getCreateSimpleTypedRepresentation(TriplesMap triplesMap) {
		return logicalSourceAspect.getLogicalSourceResolver(triplesMap).getCreateSimpleTypedRepresentation();
	}

	private RefObjectMapper createRefObjectMapper(
		RefObjectMap refObjectMap,
		CreateSimpleTypedRepresentation createSimpleTypedRepresentation
	) {
		Set<Join> joinConditions = refObjectMap.getJoinConditions();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating mapper for RefObjectMap {}", refObjectMap.getResourceName());
		}

		return new RefObjectMapper(
			createParentTriplesMapper(refObjectMap.getParentTriplesMap()),
			joinConditions,
			createSimpleTypedRepresentation
		);
	}

	public void bindInputStream(InputStream inputStream) {
		requireNonNull(
			inputStream,
			"input stream should be provided when binding stream to mapper"
		);
		bindInputStream(DEFAULT_STREAM_NAME, inputStream);
	}

	public void bindInputStream(String name, InputStream inputStream) {
		requireNonNull(
			name,
			"Name should be specified when binding named stream to mapper"
		);
		requireNonNull(
			inputStream,
			"input stream should be provided when binding named stream to mapper"
		);

		sourceManager.addSource(name, inputStream);
	}

}
