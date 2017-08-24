package com.taxonic.rml.engine;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

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

import com.taxonic.rml.engine.template.Template;
import com.taxonic.rml.engine.template.TemplateParser;
import com.taxonic.rml.model.GraphMap;
import com.taxonic.rml.model.ObjectMap;
import com.taxonic.rml.model.PredicateMap;
import com.taxonic.rml.model.SubjectMap;
import com.taxonic.rml.model.TermMap;
import com.taxonic.rml.model.TermType;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.util.IriEncoder;

class TermGeneratorCreator {
	
	private ValueFactory f;
	private String baseIri;
	private Function<String, String> encodeIri;
	private TemplateParser templateParser;
	private RmlMapper mapper;

	static TermGeneratorCreator create(RmlMapper mapper) {
		return new TermGeneratorCreator(
			SimpleValueFactory.getInstance(),
			"http://none.com/",
			IriEncoder.create(),
			TemplateParser.build(),
			mapper
		);
	}
	
	TermGeneratorCreator(
		ValueFactory valueFactory,
		String baseIri,
		Function<String, String> encodeIri,
		TemplateParser templateParser,
		RmlMapper mapper
	) {
		this.f = valueFactory;
		this.baseIri = baseIri;
		this.encodeIri = encodeIri;
		this.templateParser = templateParser;
		this.mapper = mapper;
	}

	@SuppressWarnings("unchecked")
	TermGenerator<Value> getObjectGenerator(ObjectMap map) {
		return (TermGenerator<Value>) getGenerator(
			map,
			Arrays.asList(TermType.IRI, TermType.BLANK_NODE, TermType.LITERAL),
			Arrays.asList(IRI.class, Literal.class)
		);
	}

	@SuppressWarnings("unchecked")
	TermGenerator<IRI> getGraphGenerator(GraphMap map) {
		return (TermGenerator<IRI>) getGenerator(
			map,
			Arrays.asList(TermType.IRI),
			Arrays.asList(IRI.class)
		);
	}
	
	@SuppressWarnings("unchecked")
	TermGenerator<IRI> getPredicateGenerator(PredicateMap map) {
		return (TermGenerator<IRI>) getGenerator(
			map,
			Arrays.asList(TermType.IRI),
			Arrays.asList(IRI.class)
		);
	}
	
	@SuppressWarnings("unchecked")
	TermGenerator<Resource> getSubjectGenerator(SubjectMap map) {
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
		return
		Arrays.<Supplier<Optional<TermGenerator<Value>>>>asList(
			
			// constant
			() -> getConstantGenerator(map, allowedConstantTypes),
			
			// reference
			() -> getReferenceGenerator(map, allowedTermTypes),
			
			// template
			() -> getTemplateGenerator(map, allowedTermTypes),
			
			// functionValue
			() -> getFunctionValueGenerator(map, allowedTermTypes)
			
		)
		.stream()
		.map(s -> s.get())
		.filter(o -> o.isPresent())
		.map(o -> o.get())
		.findFirst()
		.orElseThrow(() ->
			new RuntimeException("could not create generator for map [" + map + "]"));
	}
	
	private Optional<TermGenerator<Value>> getConstantGenerator(
		TermMap map,
		List<Class<? extends Value>> allowedConstantTypes
	) {
		Value constant = map.getConstant();
		if (constant == null) return Optional.empty();
		if (allowedConstantTypes.stream().noneMatch(c -> c.isInstance(constant)))
			throw new RuntimeException("encountered constant value of type " +
				constant.getClass() + ", which is not allowed for this term map");
		return Optional.of(e -> constant);
	}
	
	private Optional<TermGenerator<Value>> getTemplateGenerator(
		TermMap map,
		List<TermType> allowedTermTypes
	) {
		
		String templateStr = map.getTemplate();
		if (templateStr == null) return Optional.empty();
		
		Template template = templateParser.parse(templateStr);
		
		TermType termType = determineTermType(map);

		// for IRI term types, make template values 'IRI-safe'.
		// otherwise, do not transform template values.
		Function<String, String> transformValue = termType == TermType.IRI
			? encodeIri
			: v -> v;
		
		Function<EvaluateExpression, Object> getValue =
			new GetTemplateValue(
				template,
				template.getExpressions(),
				transformValue,
				this::createNaturalRdfLexicalForm
			);
			
		return Optional.of(getGenerator(
			map,
			getValue,
			allowedTermTypes,
			termType
		));
	}

	private Optional<TermGenerator<Value>> getReferenceGenerator(
		TermMap map,
		List<TermType> allowedTermTypes
	) {
		
		String reference = map.getReference();
		if (reference == null) return Optional.empty();
		
		Function<EvaluateExpression, Object> getValue =
			evaluateExpression -> evaluateExpression.apply(reference);
		
		return Optional.of(getGenerator(
			map,
			getValue,
			allowedTermTypes,
			determineTermType(map)
		));
	}
	
	private Optional<TermGenerator<Value>> getFunctionValueGenerator(
		TermMap map,
		List<TermType> allowedTermTypes
	) {
		
		TriplesMap executionMap = map.getFunctionValue();
		SubjectMapper executionMapper = mapper.createSubjectMapper(executionMap);
		
		// when 'executionMap' is evaluated, the generated triples
		// describe a fno:Execution instance, which we can then execute.
		
		// TODO check that executionMap has an idential logical source

		Function<EvaluateExpression, Object> getValue =
			evaluateExpression -> {
				
				LinkedHashModel model = new LinkedHashModel();
				Resource execution = executionMapper.map(model, evaluateExpression);
				
				
				return "62";
			};
		
		return Optional.of(getGenerator(
			map,
			getValue,
			allowedTermTypes,
			determineTermType(map)
		));
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

	private TermGenerator<Value> getGenerator(
		TermMap map,
		Function<EvaluateExpression, Object> getValue,
		List<TermType> allowedTermTypes,
		TermType termType
	) {
		
		Function<
			Function<String, ? extends Value>,
			TermGenerator<Value>
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
	
	private BNode generateBNodeTerm(String lexicalForm) {
		// TODO consider hash of 'lexicalForm' instead
		String id = createValidBNodeId(lexicalForm);
		// TODO not sure if successively generated ids for the
		// same lexical form should have a different id (suffix -1, -2, -3, etc.)
		return f.createBNode(id);
	}
	
	private String createValidBNodeId(String lexicalForm) {
		StringBuilder id = new StringBuilder("bnode");
		String suffix = lexicalForm.replaceAll("[^a-zA-Z_0-9-]+", "");
		if (!suffix.isEmpty())
			id.append("-").append(suffix);
		return id.toString();
	}

	private String createNaturalRdfLexicalForm(Object value) {
		// TODO https://www.w3.org/TR/r2rml/#dfn-natural-rdf-literal
		return value.toString();
	}
	
}
