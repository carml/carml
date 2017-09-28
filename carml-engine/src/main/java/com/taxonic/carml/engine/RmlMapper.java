package com.taxonic.carml.engine;

import com.taxonic.carml.engine.function.ExecuteFunction;
import com.taxonic.carml.engine.function.Functions;
import com.taxonic.carml.logical_source_resolver.JsonPathResolver;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.BaseObjectMap;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.IoUtils;
import com.taxonic.carml.vocab.Rdf;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
	
	private static final String DEFAULT_STREAM_NAME = "DEFAULT";

	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder {
		
		private Functions functions = new Functions(); // TODO
		private List<SourceResolver> sourceResolvers = new ArrayList<>();
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
			sourceResolver(o -> resolveFilePathToInputStream(o, basePath));
			return this;
		}

		public Builder addDefaultLogicalSourceResolvers() {
			this.logicalSourceResolvers.put(Rdf.Ql.JsonPath, new JsonPathResolver());
			return this;
		}

		public Builder setLogicalSourceResolver(IRI iri, LogicalSourceResolver resolver) {
			this.logicalSourceResolvers.put(iri, resolver);
			return this;
		}

		public Builder removeLogicalSourceResolver(IRI iri) {
			this.logicalSourceResolvers.remove(iri);
			return this;
		}
		
		private Optional<InputStream> resolveFilePathToInputStream(Object object, Path basePath) {
			if (!(object instanceof String))
				return Optional.empty();
			String s = (String) object;
			Path path = basePath.resolve(s);
			InputStream inputStream;
			try {
				inputStream = Files.newInputStream(path);
			}
			catch (Exception e) {
				throw new RuntimeException("could not create input stream for path [" + path + "]");
			}
			return Optional.of(ensureResettableStream(inputStream));
		}

		public Builder classPathResolver(String basePath) {
			sourceResolver(o -> resolveClassPathToInputStream(o, basePath));
			return this;
		}
		
		private Optional<InputStream> resolveClassPathToInputStream(Object object, String basePath) {
			if (!(object instanceof String))
				return Optional.empty();
			String s = (String) object;
			
			return Optional.of(
				ensureResettableStream(
					RmlMapper.class.getClassLoader().getResourceAsStream(basePath + "/" + s)
				)
			);
		}
		
		public RmlMapper build() {
			
			CarmlStreamResolver carmlStreamResolver = new CarmlStreamResolver();

			RmlMapper mapper =
				new RmlMapper(
					new CompositeSourceResolver(
						// prepend carml stream resolver to regular resolvers
						Stream.concat(
							Stream.of(carmlStreamResolver),
							sourceResolvers.stream()
						)
						.collect(Collectors.toList())
					),
					logicalSourceResolvers,
					functions
				);
			
			// carmlStreamResolver needs a reference to the mapper, since
			// input streams will be bound by name through the mapper instance
			carmlStreamResolver.setMapper(mapper);
			
			return mapper;
		}
	}
	
	private static class CarmlStreamResolver implements SourceResolver {

		private RmlMapper mapper;
		
		public void setMapper(RmlMapper mapper) {
			this.mapper = mapper;
		}

		@Override
		public Optional<InputStream> apply(Object o) {
			
			if (!(o instanceof NameableStream))
				return Optional.empty();
			
			NameableStream stream = (NameableStream) o;
			Optional<String> name = Optional.ofNullable(stream.getStreamName());
			InputStream resolved =
				name.isPresent() ?
					mapper.getInputStream(name.get()) :
					mapper.getInputStream(DEFAULT_STREAM_NAME);
			return Optional.of(resolved);
		}
	}
	
	private static class CompositeSourceResolver implements Function<Object, InputStream> {

		private List<SourceResolver> resolvers;
		
		CompositeSourceResolver(List<SourceResolver> resolvers) {
			this.resolvers = resolvers;
		}

		@Override
		public InputStream apply(Object source) {
			return
			resolvers.stream()
				.map(r -> r.apply(source))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.findFirst()
				.orElseThrow(() -> 
					new RuntimeException(String.format("could not resolve source [%s]", source)));
		}
	}
	
	private Function<Object, InputStream> sourceResolver;

	private Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers;
	
	private TermGeneratorCreator termGenerators = TermGeneratorCreator.create(this); // TODO

	private Functions functions;

	public RmlMapper(
		Function<Object, InputStream> sourceResolver,
		Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers,
		Functions functions
	) {
		this.sourceResolver = sourceResolver;
		this.functions = functions;

		this.logicalSourceResolvers = logicalSourceResolvers;
	}

	public Optional<ExecuteFunction> getFunction(IRI iri) {
		return functions.getFunction(iri);
	}

	public Model map(Set<TriplesMap> mapping) {
		Model model = new LinkedHashModel();
		mapping.stream()
			.filter(m -> !isTriplesMapOnlyUsedAsFunctionValue(m, mapping))
			.forEach(m -> map(m, model));
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
		TriplesMapper triplesMapper = createTriplesMapper(triplesMap); // TODO cache mapper instances
		triplesMapper.map(model);
	}
	
	public String readSource(Object source) {
		//TODO: PM: ideally we should make a single pass on input streams, 
		//          but this requires some refactoring.
		//			Also for large sources, transforming to String is not viable.
		return IoUtils.readAndResetInputStream(sourceResolver.apply(source));
	}
	
	private List<TermGenerator<IRI>> createGraphGenerators(Set<GraphMap> graphMaps) {
		return graphMaps.stream()
			.map(termGenerators::getGraphGenerator)
			.collect(Collectors.toList());
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
	
	// TODO: PM: reduce cognitive complexity by splitting up in sub-methods
	private List<PredicateObjectMapper> createPredicateObjectMappers(TriplesMap triplesMap, Set<PredicateObjectMap> predicateObjectMaps) {
		return predicateObjectMaps.stream().map(m -> {
			
			Set<BaseObjectMap> objectMaps = m.getObjectMaps();
			
			List<PredicateMapper> predicateMappers =
				m.getPredicateMaps().stream().map(p -> {

					List<TermGenerator<? extends Value>> objectGenerators =
						Stream.concat(
						
							// object maps -> object generators
							getObjectMapGenerators(objectMaps),
							
							// ref object maps without joins -> object generators.
							// ref object maps without joins MUST have an identical logical source.
							getJoinlessRefObjectMapGenerators(objectMaps, triplesMap.getLogicalSource())
							
						)
						.collect(Collectors.toList());
					
					List<RefObjectMapper> refObjectMappers =
						objectMaps.stream()
							.filter(o -> o instanceof RefObjectMap)
							.map(o -> (RefObjectMap) o)
							.filter(o -> !o.getJoinConditions().isEmpty())
							.map(o -> createRefObjectMapper(o))
							.collect(Collectors.toList());

					return new PredicateMapper(
						termGenerators.getPredicateGenerator(p),
						objectGenerators,
						refObjectMappers
					);
				})
				.collect(Collectors.toList());
			
			return new PredicateObjectMapper(
				createGraphGenerators(m.getGraphMaps()),
				predicateMappers
			);
		})
		.collect(Collectors.toList());
	}
	
	SubjectMapper createSubjectMapper(TriplesMap triplesMap) {
		SubjectMap subjectMap = triplesMap.getSubjectMap();
		return
		new SubjectMapper(
			termGenerators.getSubjectGenerator(subjectMap),
			createGraphGenerators(subjectMap.getGraphMaps()),
			subjectMap.getClasses(),
			createPredicateObjectMappers(triplesMap, triplesMap.getPredicateObjectMaps())
		);
	}
	
	static class TriplesMapperComponents<T> {
		
		LogicalSourceResolver<T> logicalSourceResolver;
		private final InputStream source;
		private final String iteratorExpression;

		public TriplesMapperComponents(LogicalSourceResolver<T> logicalSourceResolver, InputStream source, String iteratorExpression) {
			this.logicalSourceResolver = logicalSourceResolver;
			this.source = source;
			this.iteratorExpression = iteratorExpression;
		}

		Supplier<Iterable<T>> getIterator() {
			return logicalSourceResolver.bindSource(source, iteratorExpression);
		}

		LogicalSourceResolver.ExpressionEvaluatorFactory<T> getExpressionEvaluatorFactory() {
			return logicalSourceResolver.getExpressionEvaluatorFactory();
		}
	}

	TriplesMapperComponents getTriplesMapperComponents(TriplesMap triplesMap) {
		
		LogicalSource logicalSource = triplesMap.getLogicalSource();

		IRI referenceFormulation = logicalSource.getReferenceFormulation();
		if (!logicalSourceResolvers.containsKey(referenceFormulation)) {
			throw new RuntimeException(String.format("Unsupported reference formulation %s", referenceFormulation));
		}

		return new TriplesMapperComponents(
			logicalSourceResolvers.get(referenceFormulation),
			sourceResolver.apply(logicalSource.getSource()),
			logicalSource.getIterator()
		);
	}
	
	private TriplesMapper createTriplesMapper(TriplesMap triplesMap) {
		
		TriplesMapperComponents components = getTriplesMapperComponents(triplesMap);
		
		return
		new TriplesMapper(
			components.getIterator(),
			components.getExpressionEvaluatorFactory(),
			createSubjectMapper(triplesMap)
		);
	}
	
	private ParentTriplesMapper createParentTriplesMapper(TriplesMap triplesMap) {
		
		TriplesMapperComponents components = getTriplesMapperComponents(triplesMap);

		return
		new ParentTriplesMapper(
			termGenerators.getSubjectGenerator(triplesMap.getSubjectMap()),
			components.getIterator(),
			components.getExpressionEvaluatorFactory()
		);
	}

	private RefObjectMapper createRefObjectMapper(RefObjectMap refObjectMap) {
		Set<Join> joinConditions = refObjectMap.getJoinConditions();
		return new RefObjectMapper(
			createParentTriplesMapper(refObjectMap.getParentTriplesMap()),
			joinConditions
		);
	}
	
	private Map<String, InputStream> inputStreams = new LinkedHashMap<>();
	
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
		
		inputStreams.put(name, ensureResettableStream(inputStream));
	}
	
	private InputStream getInputStream(String name) {
		if (!inputStreams.containsKey(name)) {
			String message = 
				name.equals(DEFAULT_STREAM_NAME) ?
					"attempting to get the bound input stream, but no binding was present" :
					String.format("attempting to get input stream by "
							+ "name [%s], but no such binding is present", name);
			throw new RuntimeException(message);
		}
		return inputStreams.get(name);
	}
	
	// We need to be able to mark and reset a stream
	private static InputStream ensureResettableStream(InputStream inputStream) {
		if (!inputStream.markSupported()) {
			// return a markable stream
			return new BufferedInputStream(inputStream);
		}
		return inputStream;
	}
	
}
