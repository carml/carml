package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.function.ExecuteFunction;
import com.taxonic.carml.engine.template.Template;
import com.taxonic.carml.engine.template.TemplateParser;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TermMap;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import com.taxonic.carml.util.IriSafeMaker;
import com.taxonic.carml.util.RdfUtil;
import com.taxonic.carml.vocab.Rdf;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.common.net.ParsedIRI;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.util.Models;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TermGeneratorCreator {

	private static final Logger LOG = LoggerFactory.getLogger(TermGeneratorCreator.class);

	private ValueFactory f;
	private String baseIri;
	private Function<String, String> makeIriSafe;
	private TemplateParser templateParser;
	private RmlMapper mapper;

	static TermGeneratorCreator create(RmlMapper mapper) {
		return new TermGeneratorCreator(
			SimpleValueFactory.getInstance(),
			"http://example.com/base/",
			IriSafeMaker.create(mapper.getNormalizationForm(), mapper.getIriUpperCasePercentEncoding()),
			TemplateParser.build(),
			mapper
		);
	}

	TermGeneratorCreator(
		ValueFactory valueFactory,
		String baseIri,
		Function<String, String> makeIriSafe,
		TemplateParser templateParser,
		RmlMapper mapper
	) {
		this.f = valueFactory;
		this.baseIri = baseIri;
		this.makeIriSafe = makeIriSafe;
		this.templateParser = templateParser;
		this.mapper = mapper;
	}

	@SuppressWarnings("unchecked")
	TermGenerator<Value> getObjectGenerator(ObjectMap map) {
		return (TermGenerator<Value>) getGenerator(
			map,
			ImmutableSet.of(TermType.IRI, TermType.BLANK_NODE, TermType.LITERAL),
			ImmutableSet.of(IRI.class, Literal.class)
		);
	}

	@SuppressWarnings("unchecked")
	TermGenerator<IRI> getGraphGenerator(GraphMap map) {
		return (TermGenerator<IRI>) getGenerator(
			map,
			ImmutableSet.of(TermType.IRI),
			ImmutableSet.of(IRI.class)
		);
	}

	@SuppressWarnings("unchecked")
	TermGenerator<IRI> getPredicateGenerator(PredicateMap map) {
		return (TermGenerator<IRI>) getGenerator(
			map,
			ImmutableSet.of(TermType.IRI),
			ImmutableSet.of(IRI.class)
		);
	}

	@SuppressWarnings("unchecked")
	TermGenerator<Resource> getSubjectGenerator(SubjectMap map) {
		return (TermGenerator<Resource>) getGenerator(
			map,
			ImmutableSet.of(TermType.BLANK_NODE, TermType.IRI),
			ImmutableSet.of(IRI.class)
		);
	}

	private TermGenerator<?> getGenerator(
		TermMap map,
		Set<TermType> allowedTermTypes,
		Set<Class<? extends Value>> allowedConstantTypes
	) {
		List<TermGenerator<Value>> generators =
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
			.map(Supplier::get)
			.filter(Optional::isPresent)
			.map(Optional::get)
			.collect(Collectors.toList());

		if (generators.isEmpty()) {
			throw new RuntimeException(String.format("No constant, reference, template or function value found for term map [%s]",
					map.getResourceName()));
		}
		if (generators.size() > 1) {
			throw new RuntimeException(String.format("%s value generators were created for term map [%s], where only 1 is expected.",
					generators.size(), map.getResourceName()));
		}
		return generators.get(0);
	}

	private Optional<TermGenerator<Value>> getConstantGenerator(
		TermMap map,
		Set<Class<? extends Value>> allowedConstantTypes
	) {
		Value constant = map.getConstant();
		if (constant == null) {
			return Optional.empty();
		}
		if (allowedConstantTypes.stream().noneMatch(c -> c.isInstance(constant))) {
			throw new RuntimeException("encountered constant value of type " +
				constant.getClass() + ", which is not allowed for this term map");
		}
		List<Value> constants = ImmutableList.of(constant);
		if (LOG.isTraceEnabled()) {
			LOG.trace("Generated constant values: {}", constants);
		}
		return Optional.of(e -> constants);
	}

	private Optional<TermGenerator<Value>> getTemplateGenerator(
		TermMap map,
		Set<TermType> allowedTermTypes
	) {

		String templateStr = map.getTemplate();
		if (templateStr == null) {
			return Optional.empty();
		}

		Template template = templateParser.parse(templateStr);

		TermType termType = determineTermType(map);

		// for IRI term types, make template values 'IRI-safe'.
		// otherwise, do not transform template values.
		Function<String, String> transformValue = termType == TermType.IRI
			? makeIriSafe
			: v -> v;

		Function<EvaluateExpression, Optional<Object>> getValue =
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
		Set<TermType> allowedTermTypes
	) {

		String reference = map.getReference();
		if (reference == null) {
			return Optional.empty();
		}

		Function<EvaluateExpression, Optional<Object>> getValue =
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
		 Set<TermType> allowedTermTypes
	) {

		// TODO: make nicer use of optional
		TriplesMap executionMap = map.getFunctionValue();
		if (executionMap == null) {
			return Optional.empty();
		}

		SubjectMapper executionMapper = mapper.createSubjectMapper(executionMap);

		// when 'executionMap' is evaluated, the generated triples
		// describe a fno:Execution instance, which we can then execute.

		// TODO check that executionMap has an identical logical source

		TermType termType = determineTermType(map);

		// for IRI term types, make values valid IRIs.
		UnaryOperator<Object> returnValueAdapter = termType == TermType.IRI
			? this::iriEncodeResult
			: v -> v;

		Function<EvaluateExpression, Optional<Object>> getValue =
			evaluateExpression -> functionEvaluation(evaluateExpression, executionMapper, returnValueAdapter);

		return Optional.of(getGenerator(
			map,
			getValue,
			allowedTermTypes,
			determineTermType(map)
		));
	}

	private Optional<Object> functionEvaluation(EvaluateExpression evaluateExpression, SubjectMapper executionMapper, UnaryOperator<Object> returnValueAdapter) {
		Model model = new LinkedHashModel();
		Optional<Resource> execution = executionMapper.map(model, evaluateExpression);

		return execution.map(e -> mapExecution(e, model, returnValueAdapter));
	}

	private Object mapExecution(Resource execution, Model model, UnaryOperator<Object> returnValueAdapter) {
		IRI functionIri = getFunctionIRI(execution, model);
		ExecuteFunction function =
				mapper.getFunction(functionIri)
					.orElseThrow(() -> new RuntimeException(
						"no function registered for function IRI [" + functionIri + "]"));

		return function.execute(model, execution, returnValueAdapter);
	}

	private Object iriEncodeResult(Object result) {
		if (result instanceof Collection<?>) {
			return ((Collection<?>) result).stream()
			.map(this::encodeAsIri)
			.collect(ImmutableCollectors.toImmutableList());
		} else {
			return encodeAsIri(result);
		}
	}

	private Object encodeAsIri(Object value) {
		String iriValue;

		if (value instanceof Value) {
			iriValue = ((Value) value).stringValue();
		} else {
			iriValue = value.toString();
		}

		// perform unicode normalization
		iriValue = Normalizer.normalize(iriValue, mapper.getNormalizationForm());

		return ParsedIRI.create(iriValue).toString();
	}

	private IRI getFunctionIRI(Resource execution, Model model) {
		return Models.objectIRI(model.filter(execution, Rdf.Fno.executes, null))
			.orElseGet(() -> Models.objectIRI(model.filter(execution, Rdf.Fno.old_executes, null))
						.orElseThrow(() -> new RuntimeException("function execution does not have fno:executes value")));
	}

	private TermType determineTermType(TermMap map) {

		TermType termType = map.getTermType();
		if (termType != null) {
			return termType;
		}

		if (map instanceof ObjectMap) {
			ObjectMap objectMap = (ObjectMap) map;
			if (
				isReferenceTermMap(map) ||
				objectMap.getLanguage() != null ||
				objectMap.getDatatype() != null
			) {
				return TermType.LITERAL;
			}
		}
		return TermType.IRI;
	}

	private boolean isReferenceTermMap(TermMap map) {
		return
			map.getConstant() == null &&
			map.getReference() != null;
	}

	private List<Value> unpackEvaluatedExpression(Object result, Function<String, ? extends Value> generateTerm) {
		if (result instanceof Collection<?>) {
			return ((Collection<?>) result).stream()
			.map(i -> generateTerm.apply(createNaturalRdfLexicalForm(i)))
			.collect(ImmutableCollectors.toImmutableList());
		} else {
			Value v = generateTerm.apply(createNaturalRdfLexicalForm(result));
			if (v == null) {
				return ImmutableList.<Value>of();
			} else {
				return ImmutableList.of(v);
			}
		}
	}

	private TermGenerator<Value> getGenerator(
		TermMap map,
		Function<EvaluateExpression, Optional<Object>> getValue,
		Set<TermType> allowedTermTypes,
		TermType termType
	) {

		Function<
			Function<String, ? extends Value>,
			TermGenerator<Value>
		> createGenerator = generateTerm ->
			evaluateExpression -> {
				Optional<Object> referenceValue = getValue.apply(evaluateExpression);
				if (LOG.isTraceEnabled()) {
					LOG.trace("with result: {}", referenceValue.map(r -> r).orElse("null"));
				}
				return referenceValue.map( r ->
						unpackEvaluatedExpression(r, generateTerm))
						.orElse(ImmutableList.of());
			};

		if (!allowedTermTypes.contains(termType)) {
			throw new RuntimeException(
					String.format("encountered disallowed term type [%s]%nin TermMap:%n%s%n%n allowed TermTypes: %s",
					termType, map, allowedTermTypes));
		}

		switch (termType) {

			case IRI:
				return createGenerator.apply(this::generateIriTerm);

			case BLANK_NODE:
				return createGenerator.apply(this::generateBNodeTerm);

			case LITERAL:

				// term map is assumed to be an object map if it has term type literal
				ObjectMap objectMap = (ObjectMap) map;

				String languageTag = objectMap.getLanguage();

				if (languageTag != null) {
					if (!Literals.isValidLanguageTag(languageTag)) {
						throw new RuntimeException(
								String.format("Invalid lang tag '%s' used in object map %n%s", languageTag, objectMap));
					}
					return createGenerator.apply(lexicalForm -> f.createLiteral(lexicalForm, languageTag));
				}

				IRI datatype = objectMap.getDatatype();
				if (datatype != null) {
					return createGenerator.apply(lexicalForm ->
						f.createLiteral(lexicalForm, datatype));
				}

				return createGenerator.apply(lexicalForm ->
					//f.createLiteral(label, datatype) // TODO infer datatype, see https://www.w3.org/TR/r2rml/#generated-rdf-term - f.e. xsd:integer for Integer instances
					f.createLiteral(lexicalForm));

			default:
				throw new RuntimeException(String.format("unknown term type [%s]%nin TermMap:%s", termType, map));

		}
	}

	private IRI generateIriTerm(String lexicalForm) {
		if (RdfUtil.isValidIri(lexicalForm)) {
			return f.createIRI(lexicalForm);
		}

		String iri = baseIri + lexicalForm;
		if (RdfUtil.isValidIri(iri)) {
			return f.createIRI(iri);
		}

		throw new RuntimeException(String.format(
				"Could not generate a valid iri from term lexical form [%s] as-is, or prefixed with base iri [%s]",
				lexicalForm, baseIri));

	}

	private BNode generateBNodeTerm(String lexicalForm) {
		// TODO consider hash of 'lexicalForm' instead
		String id = createValidBNodeId(lexicalForm);
		return f.createBNode(id);
	}

	private String createValidBNodeId(String lexicalForm) {
		StringBuilder id = new StringBuilder("bnode");
		String suffix = lexicalForm.replaceAll("[^a-zA-Z_0-9-]+", "");
		if (!suffix.isEmpty()) {
			id.append("-").append(suffix);
		}
		return id.toString();
	}

	private String createNaturalRdfLexicalForm(Object value) {
		// TODO https://www.w3.org/TR/r2rml/#dfn-natural-rdf-literal
		return value.toString();
	}

}
