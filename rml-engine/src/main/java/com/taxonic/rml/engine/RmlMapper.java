package com.taxonic.rml.engine;

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

import com.jayway.jsonpath.JsonPath;
import com.taxonic.rml.engine.function.ExecuteFunction;
import com.taxonic.rml.engine.function.Functions;
import com.taxonic.rml.model.BaseObjectMap;
import com.taxonic.rml.model.GraphMap;
import com.taxonic.rml.model.Join;
import com.taxonic.rml.model.LogicalSource;
import com.taxonic.rml.model.ObjectMap;
import com.taxonic.rml.model.PredicateObjectMap;
import com.taxonic.rml.model.RefObjectMap;
import com.taxonic.rml.model.SubjectMap;
import com.taxonic.rml.model.TriplesMap;

// TODO cache results of evaluated expressions when filling a single template, in case of repeated expressions

// TODO rr:defaultGraph

// TODO template strings should be validated during the validation step?

/* TODO re-use the ***Mapper instances for equal corresponding ***Map instances.
 * f.e. if there are 2 equal PredicateMaps in the RML mapping file,
 * re-use the same PredicateMapper instance
 */

public class RmlMapper {

	private Function<String, InputStream> sourceResolver;
	
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

	public Model map(List<TriplesMap> mapping) {
		Model model = new LinkedHashModel();
		mapping.forEach(m -> map(m, model));
		return model;
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
	
	private Stream<TermGenerator<Value>> getJoinlessRefObjectMapGenerators(
		Set<BaseObjectMap> objectMaps, LogicalSource logicalSource
	) {
		return objectMaps.stream()
			.filter(o -> o instanceof RefObjectMap)
			.map(o -> (RefObjectMap) o)
			.filter(o -> o.getJoinConditions().isEmpty())
			.map(o -> checkLogicalSource(o, logicalSource))
			.map(o -> (TermGenerator<Value>) (Object) // TODO not very nice
				createRefObjectJoinlessMapper(o));
	}
	
	private TermGenerator<Resource> createRefObjectJoinlessMapper(RefObjectMap refObjectMap) {
		return termGenerators.getSubjectGenerator(
			refObjectMap.getParentTriplesMap().getSubjectMap()
		);
	}
	
	private List<PredicateObjectMapper> createPredicateObjectMappers(TriplesMap triplesMap, Set<PredicateObjectMap> predicateObjectMaps) {
		return predicateObjectMaps.stream().map(m -> {
			
			Set<BaseObjectMap> objectMaps = m.getObjectMaps();
			
			List<PredicateMapper> predicateMappers =
				m.getPredicateMaps().stream().map(p -> {

					List<TermGenerator<Value>> objectGenerators =
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
			s -> JsonPath.read((String) s, iterator);
			
		Function<Object, EvaluateExpression> expressionEvaluatorFactory =
			object -> expression -> JsonPath.read(object, expression);
		
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
			s -> JsonPath.read((String) s, iterator);
			
		Function<Object, EvaluateExpression> expressionEvaluatorFactory =
			object -> expression -> JsonPath.read(object, expression);
		
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
