package com.taxonic.rml.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

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
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import com.jayway.jsonpath.JsonPath;
import com.taxonic.rml.engine.template.Template;
import com.taxonic.rml.engine.template.Template.Expression;
import com.taxonic.rml.engine.template.TemplateParser;
import com.taxonic.rml.model.GraphMap;
import com.taxonic.rml.model.LogicalSource;
import com.taxonic.rml.model.ObjectMap;
import com.taxonic.rml.model.PredicateMap;
import com.taxonic.rml.model.SubjectMap;
import com.taxonic.rml.model.TermMap;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.vocab.Rdf.Rr;

// TODO cache results of evaluated expressions when filling a single template, in case of repeated expressions

public class RmlMapper {

	private Function<String, InputStream> sourceResolver;
	private TemplateParser templateParser;
	private Function<String, String> encodeIri = IriEncoder.create(); // TODO
	
	public RmlMapper(
		Function<String, InputStream> sourceResolver,
		TemplateParser templateParser
	) {
		this.sourceResolver = sourceResolver;
		this.templateParser = templateParser;
	}

	public Model map(List<TriplesMap> mapping) {
		
		map(mapping.get(0));
		
		return null;
	}
	
	private void map(TriplesMap map) {
		
		
		Model model = createMapper(map);
		
		StringWriter writer = new StringWriter();
		Rio.write(model, writer, RDFFormat.TURTLE);
		System.out.println(writer.toString());
		
		
	}
	
	private String readSource(String source) {
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
	
	private Model createMapper(TriplesMap map) {
		
		LogicalSource logicalSource = map.getLogicalSource();
		logicalSource.getSource();
		logicalSource.getReferenceFormulation();
		logicalSource.getIterator();
		
		String json = readSource(logicalSource.getSource());
		
		Object value = JsonPath.read(json, logicalSource.getIterator()); 
		
		SubjectMap subjectMap = map.getSubjectMap();
		
		Function<EvaluateExpression, ? extends Value> subjectGenerator =
			getSubjectGenerator(subjectMap);
		
		// TODO use rr:graph
		
		boolean isIterable = Iterable.class.isAssignableFrom(value.getClass());
		Iterable<?> iterable = isIterable
			? (Iterable<?>) value
			: Collections.singleton(value);
		
		Model model = new LinkedHashModel();
			
		iterable.forEach(e -> {
			
			EvaluateExpression evaluate =
				x -> JsonPath.read(e, x);

			Value subject = subjectGenerator.apply(evaluate);
			System.out.println(">> " + subject);
			
			map.getPredicateObjectMaps().forEach(m -> {
				
				m.getPredicateMaps().forEach(p -> {
					
					Function<EvaluateExpression, ? extends Value> predicateGenerator =
						getPredicateGenerator(p);
					
					Value predicate = predicateGenerator.apply(evaluate);
					
					m.getObjectMaps().forEach(o -> {

						Function<EvaluateExpression, ? extends Value> objectGenerator =
							getObjectGenerator((ObjectMap) o);
						
						Value object = objectGenerator.apply(evaluate);
						
						model.add((Resource) subject, (IRI) predicate, object);
						
					});
				});
			});
		});
		
		return model;
	}
	
	private String createNaturalRdfLexicalForm(Object value) {
		// TODO https://www.w3.org/TR/r2rml/#dfn-natural-rdf-literal
		return value.toString();
	}
	
	private ValueFactory f = SimpleValueFactory.getInstance();

	private String baseIri = "http://none.com/"; // TODO ???
	
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
		List<IRI> allowedTermTypes
	) {
		
		String templateStr = map.getTemplate();
		if (templateStr == null) return null;
		
		Template template = templateParser.parse(templateStr);
		Set<Expression> expressions = template.getExpressions();
		
		IRI termType = determineTermType(map);
		
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
		List<IRI> allowedTermTypes
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
	
	// TODO return an enum
	private IRI determineTermType(TermMap map) {
		
		IRI termType = map.getTermType();
		if (termType != null) return termType;
		
		if (map instanceof ObjectMap) {
			ObjectMap objectMap = (ObjectMap) map;
			if (
				isReferenceTermMap(map) ||
				objectMap.getLanguage() != null ||
				objectMap.getDatatype() != null
			)
				return Rr.Literal;
		}
		return Rr.IRI;
	}
	
	private boolean isReferenceTermMap(TermMap map) {
		return
			map.getConstant() == null &&
			map.getReference() != null;
	}
	
	private TermGenerator<?> getGenerator(
		TermMap map,
		Function<EvaluateExpression, Object> getValue,
		List<IRI> allowedTermTypes,
		IRI termType
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
		
		if (termType.equals(Rr.IRI))
			return createGenerator.apply(this::generateIriTerm);
			
		if (termType.equals(Rr.BlankNode))
			return createGenerator.apply(this::generateBNodeTerm);
			
		if (termType.equals(Rr.Literal)) {
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
		}
		
		throw new RuntimeException("unknown term type " + termType);
	}
	
	private Function<EvaluateExpression, ? extends Value> getObjectGenerator(ObjectMap map) {
		return getGenerator(
			map,
			Arrays.asList(Rr.IRI, Rr.BlankNode, Rr.Literal),
			Arrays.asList(IRI.class, Literal.class)
		);
	}
	
	private TermGenerator<?> getGraphGenerator(GraphMap map) {
		return getGenerator(
			map,
			Arrays.asList(Rr.IRI),
			Arrays.asList(IRI.class)
		);
	}
	
	private Function<EvaluateExpression, ? extends Value> getPredicateGenerator(PredicateMap map) {
		return getGenerator(
			map,
			Arrays.asList(Rr.IRI),
			Arrays.asList(IRI.class)
		);
	}
	
	private Function<EvaluateExpression, ? extends Value> getSubjectGenerator(SubjectMap map) {
		return getGenerator(
			map,
			Arrays.asList(Rr.BlankNode, Rr.IRI),
			Arrays.asList(IRI.class)
		);
	}
	
	private TermGenerator<?> getGenerator(
		TermMap map,
		List<IRI> allowedTermTypes,
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
	
	// TODO templates should be validated on earlier..
	
	
}
