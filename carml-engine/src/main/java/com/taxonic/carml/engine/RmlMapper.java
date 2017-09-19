package com.taxonic.carml.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.taxonic.carml.engine.function.ExecuteFunction;
import com.taxonic.carml.engine.function.Functions;
import com.taxonic.carml.model.BaseObjectMap;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;

// TODO cache results of evaluated expressions when filling a single template, in case of repeated expressions

// TODO rr:defaultGraph

// TODO template strings should be validated during the validation step?

/* TODO re-use the ***Mapper instances for equal corresponding ***Map instances.
 * f.e. if there are 2 equal PredicateMaps in the RML mapping file,
 * re-use the same PredicateMapper instance
 */

public class RmlMapper {

	private Function<String, InputStream> sourceResolver;
	
	private static Configuration JSONPATH_CONF = Configuration.builder()
			   .options(Option.DEFAULT_PATH_LEAF_TO_NULL).build();
	
	private TermGeneratorCreator termGenerators = TermGeneratorCreator.create(this); // TODO

	private Functions functions = new Functions(); // TODO

	public RmlMapper(
		Function<String, InputStream> sourceResolver
	) {
		this.sourceResolver = sourceResolver;
	}

	public void addFunctions(Object fn) {
		functions.addFunctions(fn);
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
	
	public String readSource(String source) {
		try (Reader reader = new InputStreamReader(
			sourceResolver.apply(source),
			StandardCharsets.UTF_8
		)) {
			// TODO depending on transitive dependency here, because newer commons-io resulted in conflict with version used by rdf4j
			return IOUtils.toString(reader);
		}
		catch (IOException e) {
			throw new RuntimeException("error reading source [" + source + "]", e);
		}
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
	
	private TriplesMapper createTriplesMapper(TriplesMap triplesMap) {
		
		LogicalSource logicalSource = triplesMap.getLogicalSource();
		
//		logicalSource.getReferenceFormulation();
		
		// TODO this all assumes json
		
		Supplier<Object> getSource = () -> readSource(logicalSource.getSource());
		
		String iterator = logicalSource.getIterator();
		UnaryOperator<Object> applyIterator =
			s -> JsonPath.using(JSONPATH_CONF).parse((String) s).read(iterator);
		
		Function<Object, EvaluateExpression> expressionEvaluatorFactory =
			object -> expression -> Optional.ofNullable(
				JsonPath.using(JSONPATH_CONF).parse(object).read(expression));
		
		return
		new TriplesMapper(
			getSource,
			applyIterator,
			expressionEvaluatorFactory,
			createSubjectMapper(triplesMap)
		);
	}
	
	private ParentTriplesMapper createParentTriplesMapper(TriplesMap triplesMap) {
		
		LogicalSource logicalSource = triplesMap.getLogicalSource();
		
//		logicalSource.getReferenceFormulation();
		
		// TODO this all assumes json
		
		Supplier<Object> getSource = () -> readSource(logicalSource.getSource());
		
		String iterator = logicalSource.getIterator();
		UnaryOperator<Object> applyIterator =
			s -> JsonPath.using(JSONPATH_CONF).parse((String) s).read(iterator);
			
		Function<Object, EvaluateExpression> expressionEvaluatorFactory =
			object -> expression -> Optional.ofNullable(
				JsonPath.using(JSONPATH_CONF).parse(object).read(expression));
		
		return
		new ParentTriplesMapper(
			termGenerators.getSubjectGenerator(triplesMap.getSubjectMap()), 
			getSource, 
			applyIterator, 
			expressionEvaluatorFactory
		);
	}

	private RefObjectMapper createRefObjectMapper(RefObjectMap refObjectMap) {
		Set<Join> joinConditions = refObjectMap.getJoinConditions();
		return new RefObjectMapper(
			createParentTriplesMapper(refObjectMap.getParentTriplesMap()),
			joinConditions
		);
	};
	
}
