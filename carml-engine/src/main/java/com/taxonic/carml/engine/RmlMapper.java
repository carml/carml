package com.taxonic.carml.engine;

import com.taxonic.carml.engine.function.ExecuteFunction;
import com.taxonic.carml.engine.function.Functions;
import com.taxonic.carml.logical_source_resolver.CsvResolver;
import com.taxonic.carml.logical_source_resolver.JsonPathResolver;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.logical_source_resolver.XPathResolver;
import com.taxonic.carml.model.BaseObjectMap;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import com.taxonic.carml.vocab.Rdf;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

// TODO cache results of evaluated expressions when filling a single template, in case of repeated expressions

// TODO rr:defaultGraph

// TODO template strings should be validated during the validation step?

/* TODO re-use the ***Mapper instances for equal corresponding ***Map instances.
 * f.e. if there are 2 equal PredicateMaps in the RML mapping file,
 * re-use the same PredicateMapper instance
 */

public class RmlMapper {

	static final String DEFAULT_STREAM_NAME = "DEFAULT";

	private LogicalSourceManager sourceManager;

	private Function<Object, String> sourceResolver;

	private Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers;

	private TermGeneratorCreator termGenerators = TermGeneratorCreator.create(this); // TODO

	private Functions functions;

	public RmlMapper(
		Function<Object, String> sourceResolver,
		Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers,
		Functions functions
	) {
		this.sourceManager = new LogicalSourceManager();
		this.sourceResolver = sourceResolver;
		this.functions = functions;
		this.logicalSourceResolvers = logicalSourceResolvers;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {

		private Functions functions = new Functions(); // TODO
		private Set<SourceResolver> sourceResolvers = new HashSet<>();
		private Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers = new HashMap<>();

		public Builder addFunctions(Object fn) {
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

		public Builder addDefaultLogicalSourceResolvers() {
			logicalSourceResolvers.put(Rdf.Ql.JsonPath, new JsonPathResolver());
			logicalSourceResolvers.put(Rdf.Ql.XPath, new XPathResolver());
			logicalSourceResolvers.put(Rdf.Ql.Csv, new CsvResolver());
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

		public RmlMapper build() {

			CarmlStreamResolver carmlStreamResolver = new CarmlStreamResolver();

			if (logicalSourceResolvers.isEmpty()) {
				addDefaultLogicalSourceResolvers();
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
					functions
				);



			// Resolvers need a reference to the source manager, to manage
			// the caching of sources.
			compositeResolver.setSourceManager(mapper.getSourceManager());

			return mapper;
		}
	}

	private static class FileResolver implements SourceResolver {

		private LogicalSourceManager sourceManager;
		private Path basePath;

		FileResolver(Path basePath) {
			this.basePath = basePath;
		}

		public void setSourceManager(LogicalSourceManager sourceManager) {
			this.sourceManager = sourceManager;
		}

		@Override
		public Optional<String> apply(Object o) {
			if (!(o instanceof String))
				return Optional.empty();
			String fileName = (String) o;
			Path path = basePath.resolve(fileName);
			String sourceName = path.toString();

			// Cache source if not already done.
			if (!sourceManager.hasSource(sourceName)) {
				try {
					sourceManager.addSource(sourceName, new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
				} catch (IOException e) {
					throw new RuntimeException(
							"could not create file source for path [" + path + "]");
				}
			}

			return Optional.of(sourceManager.getSource(sourceName));
		}

	}

	private static class ClassPathResolver implements SourceResolver {

		private LogicalSourceManager sourceManager;
		private String basePath;

		ClassPathResolver(String basePath) {
			this.basePath = basePath;
		}

		public void setSourceManager(LogicalSourceManager sourceManager) {
			this.sourceManager = sourceManager;
		}

		@Override
		public Optional<String> apply(Object o) {
			if (!(o instanceof String)) {
				return Optional.empty();
			}
			String sourceName = basePath + "/" + (String) o;

			// Cache source if not already done.
			if (!sourceManager.hasSource(sourceName)) {
				sourceManager.addSource(sourceName,
						RmlMapper.class.getClassLoader()
								.getResourceAsStream(sourceName));
			}

			return Optional.of(sourceManager.getSource(sourceName));
		}

	}

	private static class CarmlStreamResolver implements SourceResolver {

		private LogicalSourceManager sourceManager;

		public void setSourceManager(LogicalSourceManager sourceManager) {
			this.sourceManager = sourceManager;
		}

		@Override
		public Optional<String> apply(Object o) {

			if (!(o instanceof NameableStream))
				return Optional.empty();

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

		private Set<SourceResolver> resolvers;

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
		Model model = new LinkedHashModel();
		mapping.stream()
			.filter(m -> !isTriplesMapOnlyUsedAsFunctionValue(m, mapping))
			.forEach(m -> map(m, model));
		this.sourceManager.clear();
		return model;
	}

	private boolean isTriplesMapOnlyUsedAsFunctionValue(TriplesMap map, Set<TriplesMap> mapping) {
		return
			isTriplesMapUsedAsFunctionValue(map, mapping) &&
			!isTriplesMapUsedInRefObjectMap(map, mapping);
	}

	private boolean isTriplesMapUsedAsFunctionValue(TriplesMap map, Set<TriplesMap> mapping) {

		// TODO

		return false;
	}

	private boolean isTriplesMapUsedInRefObjectMap(TriplesMap map, Set<TriplesMap> mapping) {
		return
		mapping.stream()

			// get all referencing object maps
			.flatMap(m -> m.getPredicateObjectMaps().stream())
			.flatMap(p -> p.getObjectMaps().stream())
			.filter(o -> o instanceof RefObjectMap)
			.map(o -> (RefObjectMap) o)

			// check that no referencing object map
			// has 'map' as its parent triples map
			.map(o -> o.getParentTriplesMap())
			.anyMatch(map::equals);

	}

	private void map(TriplesMap triplesMap, Model model) {
		TriplesMapper<?> triplesMapper = createTriplesMapper(triplesMap); // TODO cache mapper instances
		triplesMapper.map(model);
	}

	private Set<TermGenerator<IRI>> createGraphGenerators(Set<GraphMap> graphMaps) {
		return graphMaps.stream()
			.map(termGenerators::getGraphGenerator)
			.collect(ImmutableCollectors.toImmutableSet());
	}

	private Stream<TermGenerator<Value>> getObjectMapGenerators(
		Set<BaseObjectMap> objectMaps
	) {
		return objectMaps.stream()
			.filter(o -> o instanceof ObjectMap)
			.map(o -> termGenerators.getObjectGenerator((ObjectMap) o));
	}

	private RefObjectMap checkLogicalSource(RefObjectMap o, LogicalSource logicalSource) {
		LogicalSource parentLogicalSource = o.getParentTriplesMap().getLogicalSource();
		if (!logicalSource.equals(parentLogicalSource))
			throw new RuntimeException(
				"Logical sources are not equal.\n" +
				"Parent: " + parentLogicalSource + "\n" +
				"Child: " + logicalSource
			);
		return o;
	}

	private Stream<TermGenerator<? extends Value>> getJoinlessRefObjectMapGenerators(
		Set<BaseObjectMap> objectMaps, LogicalSource logicalSource
	) {
		return objectMaps.stream()
			.filter(o -> o instanceof RefObjectMap)
			.map(o -> (RefObjectMap) o)
			.filter(o -> o.getJoinConditions().isEmpty())
			.map(o -> checkLogicalSource(o, logicalSource))
			.map(this::createRefObjectJoinlessMapper);
	}

	private TermGenerator<Resource> createRefObjectJoinlessMapper(RefObjectMap refObjectMap) {
		return termGenerators.getSubjectGenerator(
			refObjectMap.getParentTriplesMap().getSubjectMap()
		);
	}

	private Set<PredicateObjectMapper> createPredicateObjectMappers(TriplesMap triplesMap, Set<PredicateObjectMap> predicateObjectMaps) {
		return predicateObjectMaps.stream().map(m -> {

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
		Set<TermGenerator<? extends Value>> objectGenerators =
			Stream.concat(

				// object maps -> object generators
				getObjectMapGenerators(objectMaps),

				// ref object maps without joins -> object generators.
				// ref object maps without joins MUST have an identical logical source.
				getJoinlessRefObjectMapGenerators(objectMaps, triplesMap.getLogicalSource())

			)
			.collect(ImmutableCollectors.toImmutableSet());

		Set<RefObjectMapper> refObjectMappers =
			objectMaps.stream()
				.filter(o -> o instanceof RefObjectMap)
				.map(o -> (RefObjectMap) o)
				.filter(o -> !o.getJoinConditions().isEmpty())
				.map(this::createRefObjectMapper)
				.collect(ImmutableCollectors.toImmutableSet());

		return new PredicateMapper(
			termGenerators.getPredicateGenerator(predicateMap),
			objectGenerators,
			refObjectMappers
		);
	}

	SubjectMapper createSubjectMapper(TriplesMap triplesMap) {
		SubjectMap subjectMap = triplesMap.getSubjectMap();
		if (subjectMap == null) {
			throw new RuntimeException(
					String.format("Subject map must be specified in triples map %s",
							triplesMap));
		}

		return
		new SubjectMapper(
			termGenerators.getSubjectGenerator(subjectMap),
			createGraphGenerators(subjectMap.getGraphMaps()),
			subjectMap.getClasses(),
			createPredicateObjectMappers(triplesMap, triplesMap.getPredicateObjectMaps())
		);
	}

	// TODO: Use of generic wildcard type is quite smelly, but at this point we cannot know which type
	// will be provided by the user.
	TriplesMapperComponents<?> getTriplesMapperComponents(TriplesMap triplesMap) {

		LogicalSource logicalSource = triplesMap.getLogicalSource();

		IRI referenceFormulation = logicalSource.getReferenceFormulation();
		if (!logicalSourceResolvers.containsKey(referenceFormulation)) {
			throw new RuntimeException(String.format("Unsupported reference formulation %s", referenceFormulation));
		}

		return new TriplesMapperComponents<>(
			logicalSourceResolvers.get(referenceFormulation),
			sourceResolver.apply(logicalSource.getSource()),
			logicalSource.getIterator()
		);
	}

	private TriplesMapper<?> createTriplesMapper(TriplesMap triplesMap) {

		TriplesMapperComponents<?> components = getTriplesMapperComponents(triplesMap);

		return
		new TriplesMapper<>(
			components,
			createSubjectMapper(triplesMap)
		);
	}

	private ParentTriplesMapper<?> createParentTriplesMapper(TriplesMap triplesMap) {

		TriplesMapperComponents<?> components = getTriplesMapperComponents(triplesMap);

		return
		new ParentTriplesMapper<>(
			termGenerators.getSubjectGenerator(triplesMap.getSubjectMap()),
			components
		);
	}

	private RefObjectMapper createRefObjectMapper(RefObjectMap refObjectMap) {
		Set<Join> joinConditions = refObjectMap.getJoinConditions();
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
