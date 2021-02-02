package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.function.ExecuteFunction;
import com.taxonic.carml.engine.function.Functions;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.CreateContextEvaluate;
import com.taxonic.carml.model.BaseObjectMap;
import com.taxonic.carml.model.FileSource;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.NameableStream;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer.Form;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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

	static final String DEFAULT_STREAM_NAME = "DEFAULT";

	private final LogicalSourceManager sourceManager;

	private final Function<Object, String> sourceResolver;

	private final Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers;

	Form normalizationForm;

	boolean iriUpperCasePercentEncoding;

	private final TermGeneratorCreator termGenerators;

	private final Functions functions;

	private RmlMapper(
		Function<Object, String> sourceResolver,
		Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers,
		Functions functions,
		Form normalizationForm,
		boolean iriUpperCasePercentEncoding
	) {
		this.sourceManager = new LogicalSourceManager();
		this.sourceResolver = sourceResolver;
		this.functions = functions;
		this.logicalSourceResolvers = logicalSourceResolvers;
		this.normalizationForm = normalizationForm;
		this.iriUpperCasePercentEncoding = iriUpperCasePercentEncoding;
		this.termGenerators = TermGeneratorCreator.create(this);
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

			RmlMapper mapper =
				new RmlMapper(
					compositeResolver,
					logicalSourceResolvers,
					functions,
					normalizationForm,
					iriUpperCasePercentEncoding
				);

			// Resolvers need a reference to the source manager, to manage
			// the caching of sources.
			compositeResolver.setSourceManager(mapper.getSourceManager());

			return mapper;
		}
	}

	public Form getNormalizationForm() {
		return normalizationForm;
	}

	public boolean getIriUpperCasePercentEncoding() {
		return iriUpperCasePercentEncoding;
	}

	private static Optional<String> unpackFileSource(Object sourceObject) {
		if (sourceObject instanceof String) { // Standard rml:source
			return Optional.of((String) sourceObject);
		} else if (sourceObject instanceof FileSource) { // Extended Carml source
			return Optional.of(((FileSource)sourceObject).getUrl());
		} else {
			return Optional.empty();
		}
	}

	private static class FileResolver implements SourceResolver {

		private LogicalSourceManager sourceManager;
		private final Path basePath;

		FileResolver(Path basePath) {
			this.basePath = basePath;
		}

		@Override
		public void setSourceManager(LogicalSourceManager sourceManager) {
			this.sourceManager = sourceManager;
		}

		@Override
		public Optional<String> apply(Object o) {

			return unpackFileSource(o).map(f -> {
				Path path = basePath.resolve(f);
				String sourceName = path.toString();

				// Cache source if not already done.
				if (!sourceManager.hasSource(sourceName)) {
					try {
						sourceManager.addSource(sourceName, new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
					} catch (IOException e) {
						throw new RuntimeException(String.format("could not create file source for path [%s]", path));
					}
				}

				return sourceManager.getSource(sourceName);
			});
		}

	}

	private static class ClassPathResolver implements SourceResolver {

		private LogicalSourceManager sourceManager;
		private final String basePath;

		ClassPathResolver(String basePath) {
			this.basePath = basePath;
		}

		@Override
		public void setSourceManager(LogicalSourceManager sourceManager) {
			this.sourceManager = sourceManager;
		}

		@Override
		public Optional<String> apply(Object o) {

			return unpackFileSource(o).map(f -> {
				String sourceName = basePath + "/" + f;

				// Cache source if not already done.
				if (!sourceManager.hasSource(sourceName)) {
					sourceManager.addSource(sourceName,
							RmlMapper.class.getClassLoader()
									.getResourceAsStream(sourceName));
				}

				return sourceManager.getSource(sourceName);
			});

		}

	}

	private static class CarmlStreamResolver implements SourceResolver {

		private LogicalSourceManager sourceManager;

		@Override
		public void setSourceManager(LogicalSourceManager sourceManager) {
			this.sourceManager = sourceManager;
		}

		@Override
		public Optional<String> apply(Object o) {

			if (!(o instanceof NameableStream)) {
				return Optional.empty();
			}

			NameableStream stream = (NameableStream) o;
			Optional<String> name = Optional.ofNullable(stream.getStreamName());
			String resolved =
				name.isPresent() ?
					sourceManager.getSource(name.get()) :
					sourceManager.getSource(DEFAULT_STREAM_NAME);
			return Optional.of(resolved);
		}
	}

	private static class CompositeSourceResolver implements Function<Object, String> {

		private final Set<SourceResolver> resolvers;

		CompositeSourceResolver(Set<SourceResolver> resolvers) {
			this.resolvers = resolvers;
		}

		@Override
		public String apply(Object source) {
			return
			resolvers.stream()
				.map(r -> r.apply(source))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.findFirst()
				.orElseThrow(() ->
					new RuntimeException(String.format("could not resolve source [%s]", source)));
		}

		void setSourceManager(LogicalSourceManager sourceManager) {
			resolvers.forEach(r -> r.setSourceManager(sourceManager));
		}
	}

	public LogicalSourceManager getSourceManager() {
		return this.sourceManager;
	}

	public Optional<ExecuteFunction> getFunction(IRI iri) {
		return functions.getFunction(iri);
	}

	public Model map(Set<TriplesMap> mapping) {
		validateMapping(mapping);
		Model model = new LinkedHashModel();

		Set<TriplesMap> functionValueTriplesMaps = getTermMaps(mapping)
				.filter(t -> t.getFunctionValue() != null)
				.map(TermMap::getFunctionValue)
				.collect(ImmutableCollectors.toImmutableSet());

		if(LOG.isWarnEnabled()) {
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
		this.sourceManager.clear();
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
		TriplesMapper<?> triplesMapper = createTriplesMapper(triplesMap); // TODO cache mapper instances
		triplesMapper.map(model);
	}

	private Set<TermGenerator<IRI>> createGraphGenerators(Set<GraphMap> graphMaps) {
		return graphMaps.stream()
			.map(termGenerators::getGraphGenerator)
			.collect(ImmutableCollectors.toImmutableSet());
	}

	private Stream<TermGenerator<Value>> getObjectMapGenerators(
		Set<BaseObjectMap> objectMaps, TriplesMap triplesMap
	) {
		return objectMaps.stream()
			.filter(o -> o instanceof ObjectMap)
			.peek(o -> LOG.debug("Creating term generator for ObjectMap {}", o.getResourceName()))
			.map(o -> {
				try {
					return termGenerators.getObjectGenerator((ObjectMap) o);
				} catch (RuntimeException ex) {
					throw new RuntimeException(String.format("Exception occurred for %s", exception(triplesMap, o)), ex);
				}
			});
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

	private TermGenerator<Resource> createRefObjectJoinlessMapper(RefObjectMap refObjectMap, TriplesMap triplesMap) {
		try {
			return termGenerators.getSubjectGenerator(
					refObjectMap.getParentTriplesMap().getSubjectMap()
			);
		} catch (RuntimeException ex) {
			throw new RuntimeException(String.format("Exception occurred for %s", exception(triplesMap, refObjectMap)), ex);
		}
	}

	private Set<PredicateObjectMapper> createPredicateObjectMappers(TriplesMap triplesMap, Set<PredicateObjectMap> predicateObjectMaps) {
		return predicateObjectMaps.stream()
				.peek(m -> LOG.debug("Creating mapper for PredicateObjectMap {}", m.getResourceName()))
				.map(m -> {
					Set<BaseObjectMap> objectMaps = m.getObjectMaps();

					Set<PredicateMapper> predicateMappers =
						m.getPredicateMaps().stream()
							.map(p -> createPredicateMapper(p, objectMaps, triplesMap))
							.collect(ImmutableCollectors.toImmutableSet());

					return new PredicateObjectMapper(
						createGraphGenerators(m.getGraphMaps()),
						predicateMappers
					);
				})
				.collect(ImmutableCollectors.toImmutableSet());
	}

	PredicateMapper createPredicateMapper(
		PredicateMap predicateMap,
		Set<BaseObjectMap> objectMaps,
		TriplesMap triplesMap
	) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating mapper for PredicateMap {}", log(triplesMap, predicateMap));
		}

		Set<TermGenerator<? extends Value>> objectGenerators =
			Stream.concat(

				// object maps -> object generators
				getObjectMapGenerators(objectMaps, triplesMap),

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
				.map(this::createRefObjectMapper)
				.collect(ImmutableCollectors.toImmutableSet());

		Set<NestedMapper<?>> nestedMappers = objectMaps.stream()
			.filter(o -> o instanceof NestedMapping)
			.map(o -> (NestedMapping) o)
			.map(this::createNestedMapper)
			.collect(ImmutableCollectors.toImmutableSet());

		TermGenerator<IRI> predicateGenerator;
		try{
			predicateGenerator = termGenerators.getPredicateGenerator(predicateMap);
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

		TermGenerator<Resource> subjectGenerator;
		try {
			subjectGenerator = termGenerators.getSubjectGenerator(subjectMap);
		} catch(RuntimeException ex) {
			throw new RuntimeException(String.format("Exception occurred for %s", exception(triplesMap, subjectMap)), ex);
		}

		return new SubjectMapper(
			subjectGenerator,
			createGraphGenerators(subjectMap.getGraphMaps()),
			subjectMap.getClasses(),
			createPredicateObjectMappers(triplesMap, triplesMap.getPredicateObjectMaps())
		);
	}

	<T> TriplesMapperComponents<T> getTriplesMapperComponents(TriplesMap triplesMap) {

		LogicalSource logicalSource = triplesMap.getLogicalSource();

		if (logicalSource == null) {
			throw new RuntimeException(String.format("No LogicalSource found for TriplesMap%n%s", exception(triplesMap, triplesMap)));
		}

		IRI referenceFormulation = logicalSource.getReferenceFormulation();

		if (referenceFormulation == null) {
			throw new RuntimeException(
				String.format("No reference formulation found for LogicalSource %s", exception(triplesMap, logicalSource)));
		}

		if (!logicalSourceResolvers.containsKey(referenceFormulation)) {
			throw new RuntimeException(
				String.format("Unsupported reference formulation %s in LogicalSource %s", referenceFormulation,
					exception(triplesMap, logicalSource)));
		}

		return new TriplesMapperComponents<T>(
			triplesMap.getResourceName(),
			(LogicalSourceResolver<T>) logicalSourceResolvers.get(referenceFormulation),
			logicalSource,
			sourceResolver,
			null
		);
	}

	private <T> TriplesMapper<T> createTriplesMapper(TriplesMap triplesMap) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating mapper for TriplesMap {}", triplesMap.getResourceName());
		}

		return
		new TriplesMapper<>(
			getTriplesMapperComponents(triplesMap),
			createSubjectMapper(triplesMap),
			createNestedMappers(triplesMap)
		);
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

		TriplesMapperComponents<T> components = getTriplesMapperComponents(triplesMap);

		ContextTriplesMapper<T> triplesMapper = new ContextTriplesMapper<>(
			components.getName(),
			components.createGetIterableFromContext(triplesMap.getLogicalSource().getIterator()),
			components.getExpressionEvaluatorFactory(),
			createSubjectMapper(triplesMap),
			createNestedMappers(triplesMap)
		);

		CreateContextEvaluate createContextEvaluate = components.getLogicalSourceResolver().getCreateContextEvaluate();

		return new NestedMapper<>(triplesMapper, nestedMapping.getContextEntries(), createContextEvaluate);
	}

	private ParentTriplesMapper<?> createParentTriplesMapper(TriplesMap triplesMap) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating mapper for ParentTriplesMap {}", triplesMap.getResourceName());
		}

		TriplesMapperComponents<?> components = getTriplesMapperComponents(triplesMap);

		try {
			return new ParentTriplesMapper<>(termGenerators.getSubjectGenerator(triplesMap.getSubjectMap()), components);
		}
		catch(RuntimeException ex) {
			throw new RuntimeException(String.format("Exception occurred for %s", exception(triplesMap, triplesMap.getSubjectMap())), ex);
		}
	}

	private RefObjectMapper createRefObjectMapper(RefObjectMap refObjectMap) {
		Set<Join> joinConditions = refObjectMap.getJoinConditions();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating mapper for RefObjectMap {}", refObjectMap.getResourceName());
		}

		return new RefObjectMapper(
			createParentTriplesMapper(refObjectMap.getParentTriplesMap()),
			joinConditions
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
