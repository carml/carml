package com.taxonic.rml.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import com.jayway.jsonpath.JsonPath;
import com.taxonic.rml.engine.template.Template;
import com.taxonic.rml.engine.template.Template.Expression;
import com.taxonic.rml.engine.template.TemplateParser;
import com.taxonic.rml.model.GraphMap;
import com.taxonic.rml.model.Join;
import com.taxonic.rml.model.LogicalSource;
import com.taxonic.rml.model.ObjectMap;
import com.taxonic.rml.model.PredicateMap;
import com.taxonic.rml.model.PredicateObjectMap;
import com.taxonic.rml.model.RefObjectMap;
import com.taxonic.rml.model.SubjectMap;
import com.taxonic.rml.model.TermMap;
import com.taxonic.rml.model.TermType;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.vocab.Rdf.Rr;

// TODO cache results of evaluated expressions when filling a single template, in case of repeated expressions

// TODO rr:defaultGraph

// TODO template strings should be validated during the validation step?

/* TODO re-use the ***Mapper instances for equal corresponding ***Map instances.
 * f.e. if there are 2 equal PredicateMaps in the RML mapping file,
 * re-use the same PredicateMapper instance
 */

public class RmlMapper {

	private ValueFactory f = SimpleValueFactory.getInstance();

	private String baseIri = "http://none.com/"; // TODO ???
	
	private Function<String, String> encodeIri = IriEncoder.create(); // TODO
	
	private Function<String, InputStream> sourceResolver;
	private TemplateParser templateParser;
	
	public RmlMapper(
		Function<String, InputStream> sourceResolver,
		TemplateParser templateParser
	) {
		this.sourceResolver = sourceResolver;
		this.templateParser = templateParser;
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
			.map(this::getGraphGenerator)
			.collect(Collectors.toList());
	}
	
	private List<PredicateObjectMapper> createPredicateObjectMappers(TriplesMap triplesMap, Set<PredicateObjectMap> predicateObjectMaps) {
		return predicateObjectMaps.stream().map(m -> {
			
			List<PredicateMapper> predicateMappers =
				m.getPredicateMaps().stream().map(p -> {

					List<TermGenerator<Value>> objectGenerators =
						Stream.concat(
						
							// object maps -> object generators
							m.getObjectMaps().stream()
								.filter(o -> o instanceof ObjectMap)
								.map(o -> getObjectGenerator((ObjectMap) o)),
							
							// ref objects maps without joins -> object generators
							m.getObjectMaps().stream()
								.filter(o -> o instanceof RefObjectMap)
								.map(o -> (RefObjectMap) o)
								.filter(o -> o.getJoinConditions().isEmpty())
								.map(o -> {
									LogicalSource parentLogicalSource = o.getParentTriplesMap().getLogicalSource();
									if (!triplesMap.getLogicalSource().equals(parentLogicalSource)) {
										throw new RuntimeException("Logical sources are not equal. \n Parent: " + parentLogicalSource + ", Child: " + triplesMap.getLogicalSource());
									}
									return o;
								})
								.map(o -> (TermGenerator<Value>) (TermGenerator) // TODO not very nice
									createRefObjectJoinlessMapper(o))
						)
						.collect(Collectors.toList());
					
					List<RefObjectMapper> refObjectMappers =
							m.getObjectMaps().stream()
								.filter(o -> o instanceof RefObjectMap)
								.map(o -> (RefObjectMap) o)
								.filter(o -> !o.getJoinConditions().isEmpty())
								.map(o -> createRefObjectMapper(o))
								.collect(Collectors.toList());

					return new PredicateMapper(
						getPredicateGenerator(p),
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
	
	private SubjectMapper createSubjectMapper(TriplesMap triplesMap) {
		SubjectMap subjectMap = triplesMap.getSubjectMap();
		return
		new SubjectMapper(
			getSubjectGenerator(subjectMap),
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
				getSubjectGenerator(triplesMap.getSubjectMap()), 
				getSource, 
				applyIterator, 
				expressionEvaluatorFactory);
				
	}
	
	private String createNaturalRdfLexicalForm(Object value) {
		// TODO https://www.w3.org/TR/r2rml/#dfn-natural-rdf-literal
		return value.toString();
	}
	
	private boolean isValidIri(String str) {
		return str.contains(":");
	}
	
	private IRI generateIriTerm(String lexicalForm) {
		
		if (isValidIri(lexicalForm))
			return f.createIRI(lexicalForm);
			
		String iri = baseIri + lexicalForm;
		if (isValidIri(iri))
			return f.createIRI(iri);
		
		throw new RuntimeException("data error: could not generate a valid iri from term lexical form [" + lexicalForm + "] as-is, or prefixed with base iri [" + baseIri + "]");
		
	}
	
	private static int nextId = 0;
	
	private BNode generateBNodeTerm(String lexicalForm) {
		String id = (nextId ++) + ""; // TODO hash of 'lexicalForm'
		return f.createBNode(id);
	}
	
	private Value getConstant(TermMap map, List<Class<? extends Value>> allowedConstantTypes) {
		Value constant = map.getConstant();
		if (constant == null) return null;
		if (allowedConstantTypes.stream().noneMatch(c -> c.isInstance(constant)))
			throw new RuntimeException("encountered constant value of type " + constant.getClass() + ", which is not allowed for this term map");
		return constant; // constant MUST be an IRI for subject/predicate/graph map
	}
	
	private TermGenerator<?> getTemplateGenerator(
		TermMap map,
		List<TermType> allowedTermTypes
	) {
		
		String templateStr = map.getTemplate();
		if (templateStr == null) return null;
		
		Template template = templateParser.parse(templateStr);
		Set<Expression> expressions = template.getExpressions();
		
		TermType termType = determineTermType(map);
		
		Function<EvaluateExpression, Object> getValue =
			evaluateExpression -> {
				Template.Builder templateBuilder = template.newBuilder();
				expressions.forEach(expression ->
					templateBuilder.bind(
						expression,
						prepareValueForTemplate(
							evaluateExpression.apply(expression.getValue()),
							termType.equals(Rr.IRI)
						)
					)
				);
				return templateBuilder.create();
			};
			
		return getGenerator(
			map,
			getValue,
			allowedTermTypes,
			termType
		);	
	}
	
	/**
	 * See https://www.w3.org/TR/r2rml/#from-template
	 * @param raw
	 * @return
	 */
	private String prepareValueForTemplate(Object raw, boolean makeIriSafe) {
		if (raw == null) return "NULL";
		String value = createNaturalRdfLexicalForm(raw);
		if (makeIriSafe)
			return encodeIri.apply(value);
		return value;
	}
	
	private TermGenerator<?> getReferenceGenerator(
		TermMap map,
		List<TermType> allowedTermTypes
	) {
		
		String reference = map.getReference();
		if (reference == null) return null;
		
		Function<EvaluateExpression, Object> getValue =
			evaluateExpression -> evaluateExpression.apply(reference);
		
		return getGenerator(
			map,
			getValue,
			allowedTermTypes,
			determineTermType(map)
		);
	}
	
	private TermType determineTermType(TermMap map) {
		
		TermType termType = map.getTermType();
		if (termType != null) return termType;
		
		if (map instanceof ObjectMap) {
			ObjectMap objectMap = (ObjectMap) map;
			if (
				isReferenceTermMap(map) ||
				objectMap.getLanguage() != null ||
				objectMap.getDatatype() != null
			)
				return TermType.LITERAL;
		}
		return TermType.IRI;
	}
	
	private boolean isReferenceTermMap(TermMap map) {
		return
			map.getConstant() == null &&
			map.getReference() != null;
	}
	
	private TermGenerator<?> getGenerator(
		TermMap map,
		Function<EvaluateExpression, Object> getValue,
		List<TermType> allowedTermTypes,
		TermType termType
	) {
		
		Function<
			Function<String, ? extends Value>,
			TermGenerator<?>
		> createGenerator = generateTerm ->
			evaluateExpression -> {
				Object referenceValue = getValue.apply(evaluateExpression);
				if (referenceValue == null) return null;
				String lexicalForm = createNaturalRdfLexicalForm(referenceValue);
				return generateTerm.apply(lexicalForm);
			};

		if (!allowedTermTypes.contains(termType))
			throw new RuntimeException("encountered disallowed term type [" + termType + "]; allowed term types: " + allowedTermTypes);
		
		switch (termType) {
			
			case IRI:
				return createGenerator.apply(this::generateIriTerm);
				
			case BLANK_NODE:
				return createGenerator.apply(this::generateBNodeTerm);
				
			case LITERAL:
				
				// term map is assumed to be an object map if it has term type literal
				ObjectMap objectMap = (ObjectMap) map;
				
				String language = objectMap.getLanguage();
				if (language != null)
					return createGenerator.apply(lexicalForm ->
					f.createLiteral(lexicalForm, language));
				
				IRI datatype = objectMap.getDatatype();
				if (datatype != null)
					return createGenerator.apply(lexicalForm ->
					f.createLiteral(lexicalForm, datatype));
				
				return createGenerator.apply(lexicalForm ->
				//f.createLiteral(label, datatype) // TODO infer datatype, see https://www.w3.org/TR/r2rml/#generated-rdf-term - f.e. xsd:integer for Integer instances
				f.createLiteral(lexicalForm));
				
			default:
				throw new RuntimeException("unknown term type " + termType);
				
		}
	}
	
	@SuppressWarnings("unchecked")
	private TermGenerator<Value> getObjectGenerator(ObjectMap map) {
		return (TermGenerator<Value>) getGenerator(
			map,
			Arrays.asList(TermType.IRI, TermType.BLANK_NODE, TermType.LITERAL),
			Arrays.asList(IRI.class, Literal.class)
		);
	}
	
	private RefObjectMapper createRefObjectMapper(RefObjectMap refObjectMap) {
		Set<Join> joinConditions = refObjectMap.getJoinConditions();
		
		return new RefObjectMapper(
					createParentTriplesMapper(refObjectMap.getParentTriplesMap()),
					joinConditions
				);
	};
	
	private TermGenerator<Resource> createRefObjectJoinlessMapper(RefObjectMap refObjectMap) {
		return getSubjectGenerator(refObjectMap.getParentTriplesMap().getSubjectMap());
	}
	
	@SuppressWarnings("unchecked")
	private TermGenerator<IRI> getGraphGenerator(GraphMap map) {
		return (TermGenerator<IRI>) getGenerator(
			map,
			Arrays.asList(TermType.IRI),
			Arrays.asList(IRI.class)
		);
	}
	
	@SuppressWarnings("unchecked")
	private TermGenerator<IRI> getPredicateGenerator(PredicateMap map) {
		return (TermGenerator<IRI>) getGenerator(
			map,
			Arrays.asList(TermType.IRI),
			Arrays.asList(IRI.class)
		);
	}
	
	@SuppressWarnings("unchecked")
	private TermGenerator<Resource> getSubjectGenerator(SubjectMap map) {
		return (TermGenerator<Resource>) getGenerator(
			map,
			Arrays.asList(TermType.BLANK_NODE, TermType.IRI),
			Arrays.asList(IRI.class)
		);
	}
	
	private TermGenerator<?> getGenerator(
		TermMap map,
		List<TermType> allowedTermTypes,
		List<Class<? extends Value>> allowedConstantTypes
	) {
		
		// constant
		Value constant = getConstant(map, allowedConstantTypes);
		if (constant != null) return x -> constant;

		TermGenerator<?> generator;	
		
		// reference
		generator = getReferenceGenerator(map, allowedTermTypes);
		if (generator != null) return generator;
			
		// template
		generator = getTemplateGenerator(map, allowedTermTypes);
		if (generator != null) return generator;
		
		throw new RuntimeException("could not create generator for map [" + map + "]");
	}
	
}
