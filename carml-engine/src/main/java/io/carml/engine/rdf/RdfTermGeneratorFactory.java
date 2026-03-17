package io.carml.engine.rdf;

import static io.carml.model.TermType.BLANK_NODE;
import static io.carml.model.TermType.LITERAL;
import static io.carml.util.LogUtil.exception;

import io.carml.engine.MappedValue;
import io.carml.engine.TermGenerator;
import io.carml.engine.TermGeneratorFactory;
import io.carml.engine.TermGeneratorFactoryException;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.DatatypeMap;
import io.carml.model.ExpressionMap;
import io.carml.model.GatherMap;
import io.carml.model.GraphMap;
import io.carml.model.LanguageMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TermMap;
import io.carml.model.TermType;
import io.carml.util.IriSafeMaker;
import io.carml.util.RdfValues;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;

@Slf4j
public class RdfTermGeneratorFactory implements TermGeneratorFactory<Value> {

    private static final ValueFactory SIMPLE_VALUE_FACTORY = SimpleValueFactory.getInstance();

    private final RdfTermGeneratorConfig rdfTermGeneratorConfig;

    private final ValueFactory valueFactory;

    private final UnaryOperator<String> makeIriSafe;

    private final UnaryOperator<String> makeUriSafe;

    private final Map<RefObjectMap, String> refObjectMapPrefixes;

    private RdfTermGeneratorFactory(
            RdfTermGeneratorConfig rdfTermGeneratorConfig,
            ValueFactory valueFactory,
            UnaryOperator<String> makeIriSafe,
            UnaryOperator<String> makeUriSafe,
            Map<RefObjectMap, String> refObjectMapPrefixes) {
        this.rdfTermGeneratorConfig = rdfTermGeneratorConfig;
        this.valueFactory = valueFactory;
        this.makeIriSafe = makeIriSafe;
        this.makeUriSafe = makeUriSafe;
        this.refObjectMapPrefixes = refObjectMapPrefixes;
    }

    public static RdfTermGeneratorFactory of(RdfTermGeneratorConfig rdfTermGeneratorConfig) {
        return new RdfTermGeneratorFactory(
                rdfTermGeneratorConfig,
                rdfTermGeneratorConfig.getValueFactory(),
                IriSafeMaker.create(
                        rdfTermGeneratorConfig.getNormalizationForm(),
                        rdfTermGeneratorConfig.isIriUpperCasePercentEncoding()),
                IriSafeMaker.createUriSafe(rdfTermGeneratorConfig.getNormalizationForm()),
                Map.of());
    }

    /**
     * Returns a new factory that includes the given RefObjectMap-to-prefix mappings. Used for
     * LogicalView-based mappers where joined RefObjectMaps in gather maps need expression prefix
     * resolution.
     */
    public RdfTermGeneratorFactory withRefObjectMapPrefixes(Map<RefObjectMap, String> prefixes) {
        return new RdfTermGeneratorFactory(rdfTermGeneratorConfig, valueFactory, makeIriSafe, makeUriSafe, prefixes);
    }

    private void validateTermType(TermMap termMap, Set<TermType> allowedTermTypes) {
        if (termMap.getTermType() != null && !allowedTermTypes.contains(termMap.getTermType())) {
            throw new TermGeneratorFactoryException(String.format(
                    "encountered term type %s, which is not allowed for term map%n%s",
                    termMap.getTermType(), exception(termMap)));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<Resource> getSubjectGenerator(SubjectMap subjectMap) {
        if (isBlankSubjectMap(subjectMap)) {
            return (expressionEvaluation, datatypeMapper) -> List.of(RdfMappedValue.of(valueFactory.createBNode()));
        }

        validateTermType(subjectMap, Set.of(BLANK_NODE, TermType.IRI, TermType.URI, TermType.UNSAFE_IRI));
        return (TermGenerator<Resource>) createTermGenerator(subjectMap, Set.of(IRI.class));
    }

    private boolean isBlankSubjectMap(SubjectMap subjectMap) {
        return subjectMap == null
                || (subjectMap.getConstant() == null
                        && subjectMap.getReferenceExpressionSet().isEmpty()
                        && subjectMap.getGathers().isEmpty()
                        && subjectMap.getFunctionExecution() == null
                        && subjectMap.getFunctionValue() == null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<IRI> getPredicateGenerator(PredicateMap predicateMap) {
        validateTermType(predicateMap, Set.of(TermType.IRI, TermType.URI, TermType.UNSAFE_IRI));
        return (TermGenerator<IRI>) createTermGenerator(predicateMap, Set.of(IRI.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<Value> getObjectGenerator(ObjectMap objectMap) {
        validateTermType(objectMap, Set.of(TermType.IRI, TermType.URI, TermType.UNSAFE_IRI, BLANK_NODE, LITERAL));

        var generateOverrideDatatypes =
                objectMap.getDatatypeMap() == null ? null : getDatatypeGenerator(objectMap.getDatatypeMap());

        return (TermGenerator<Value>) createTermGenerator(
                objectMap, Set.of(IRI.class, Literal.class), generateOverrideDatatypes, objectMap.getLanguageMap());
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<Resource> getGraphGenerator(GraphMap graphMap) {
        validateTermType(graphMap, Set.of(TermType.IRI, TermType.URI, TermType.UNSAFE_IRI));
        return (TermGenerator<Resource>) createTermGenerator(graphMap, Set.of(IRI.class));
    }

    @SuppressWarnings("unchecked")
    private TermGenerator<IRI> getDatatypeGenerator(DatatypeMap map) {
        return (TermGenerator<IRI>) createTermGenerator(map, Set.of(IRI.class));
    }

    private TermGenerator<? extends Value> createTermGenerator(
            TermMap termMap, Set<Class<? extends Value>> allowedConstantTypes) {
        return createTermGenerator(termMap, allowedConstantTypes, null, null);
    }

    private TermGenerator<? extends Value> createTermGenerator(
            TermMap termMap,
            Set<Class<? extends Value>> allowedConstantTypes,
            TermGenerator<? extends Value> generateOverrideDatatypes,
            LanguageMap languageMap) {
        if (termMap instanceof GatherMap gatherMap && !gatherMap.getGathers().isEmpty()) {
            return getGatherMapGenerater(gatherMap);
        } else if (termMap.getConstant() != null) {
            return getConstantGenerator(termMap, allowedConstantTypes);
        } else if (termMap.getReference() != null) {
            return getReferenceGenerator(termMap, generateOverrideDatatypes, languageMap);
        } else if (termMap.getTemplate() != null) {
            return getTemplateGenerator(termMap, generateOverrideDatatypes, languageMap);
        } else if (termMap.getFunctionExecution() != null) {
            return getFunctionExecutionGenerator(termMap, generateOverrideDatatypes, languageMap);
        } else if (termMap.getFunctionValue() != null) {
            return getFunctionExecutionGenerator(termMap, generateOverrideDatatypes, languageMap);
        } else {
            throw new TermGeneratorFactoryException(
                    String.format("No valid expression found in %s", exception(termMap)));
        }
    }

    private TermGenerator<? extends Value> getGatherMapGenerater(GatherMap gatherMap) {
        return (expressionEvaluation, datatypeMapper) -> RdfListOrContainerGenerator.of(
                        gatherMap, valueFactory, this, refObjectMapPrefixes)
                .apply(expressionEvaluation, datatypeMapper);
    }

    private TermGenerator<? extends Value> getConstantGenerator(
            TermMap termMap, Set<Class<? extends Value>> allowedConstantTypes) {
        Value constant = termMap.getConstant();
        if (allowedConstantTypes.stream().noneMatch(allowed -> allowed.isInstance(constant))) {
            throw new TermGeneratorFactoryException(String.format(
                    "encountered constant value of type %s, which is not allowed for this term map",
                    constant.getClass().getSimpleName()));
        }

        // Pre-evaluate: constant terms produce the same result for every row.
        var preComputed = List.of(RdfMappedValue.of(constant, termMap.getTargets()));
        return (expressionEvaluation, datatypeMapper) -> preComputed;
    }

    private TermGenerator<? extends Value> getReferenceGenerator(
            TermMap termMap, TermGenerator<? extends Value> generateOverrideDatatypes, LanguageMap languageMap) {
        var reference = termMap.getReference();
        var evaluation = createEvaluation(termMap);

        return createEvaluatingGenerator(evaluation, termMap, generateOverrideDatatypes, languageMap,
                (expressionEvaluation, datatypeMapper) -> datatypeMapper == null
                        ? null
                        : datatypeMapper.apply(reference).orElse(null));
    }

    private TermGenerator<? extends Value> getTemplateGenerator(
            TermMap termMap, TermGenerator<? extends Value> generateOverrideDatatypes, LanguageMap languageMap) {
        UnaryOperator<String> valueTransformingFunction =
                switch (termMap.getTermType()) {
                    case IRI -> makeIriSafe;
                    case URI -> makeUriSafe;
                    case UNSAFE_IRI, BLANK_NODE, LITERAL -> UnaryOperator.identity();
                };

        var evaluation = RdfExpressionMapEvaluation.builder()
                .expressionMap(termMap)
                .functionRegistry(rdfTermGeneratorConfig.getFunctionRegistry())
                .normalizationForm(rdfTermGeneratorConfig.getNormalizationForm())
                .rdfTermGeneratorFactory(this)
                .templateReferenceValueTransformingFunction(valueTransformingFunction)
                .iriSafeFieldNames(rdfTermGeneratorConfig.getIriSafeFieldNames())
                .build();

        return createEvaluatingGenerator(evaluation, termMap, generateOverrideDatatypes, languageMap, null);
    }

    private TermGenerator<? extends Value> getFunctionExecutionGenerator(
            TermMap termMap, TermGenerator<? extends Value> generateOverrideDatatypes, LanguageMap languageMap) {
        var evaluation = createEvaluation(termMap);
        return createEvaluatingGenerator(evaluation, termMap, generateOverrideDatatypes, languageMap, null);
    }

    /**
     * Creates a term generator that evaluates values from the pre-built evaluation, resolves
     * override datatypes and language tags, and generates RDF terms. The optional
     * {@code mappedDatatypeResolver} provides per-row mapped datatype lookup (for reference
     * generators); when null, no mapped datatype is applied.
     */
    private TermGenerator<? extends Value> createEvaluatingGenerator(
            RdfExpressionMapEvaluation evaluation,
            TermMap termMap,
            TermGenerator<? extends Value> generateOverrideDatatypes,
            LanguageMap languageMap,
            BiFunction<ExpressionEvaluation, DatatypeMapper, IRI> mappedDatatypeResolver) {
        var languageEvaluation = languageMap != null ? createEvaluation(languageMap) : null;

        return (expressionEvaluation, datatypeMapper) -> {
            var values = evaluation.evaluate(expressionEvaluation, datatypeMapper, Object.class);
            var mappedDatatype = mappedDatatypeResolver != null
                    ? mappedDatatypeResolver.apply(expressionEvaluation, datatypeMapper)
                    : null;
            var overrideDatatypes =
                    getOverrideDatatypes(generateOverrideDatatypes, expressionEvaluation, datatypeMapper);
            var languageTags = getLanguageTags(languageEvaluation, expressionEvaluation, datatypeMapper);
            return generateTerms(values, termMap, mappedDatatype, overrideDatatypes, languageTags);
        };
    }

    private List<IRI> getOverrideDatatypes(
            TermGenerator<? extends Value> generateOverrideDatatypes,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        return generateOverrideDatatypes != null
                ? generateOverrideDatatypes.apply(expressionEvaluation, datatypeMapper).stream()
                        .map(MappedValue::getValue)
                        .map(IRI.class::cast)
                        .toList()
                : List.of();
    }

    private List<String> getLanguageTags(
            RdfExpressionMapEvaluation languageEvaluation,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        if (languageEvaluation == null) {
            return List.of();
        }

        return languageEvaluation.evaluate(expressionEvaluation, datatypeMapper, String.class);
    }

    /**
     * Creates a reusable {@link RdfExpressionMapEvaluation} with the factory's common configuration.
     * The returned instance holds only immutable fields and can be reused across rows by passing
     * per-row {@code expressionEvaluation} and {@code datatypeMapper} to
     * {@link RdfExpressionMapEvaluation#evaluate}.
     */
    private RdfExpressionMapEvaluation createEvaluation(ExpressionMap expressionMap) {
        return RdfExpressionMapEvaluation.builder()
                .expressionMap(expressionMap)
                .functionRegistry(rdfTermGeneratorConfig.getFunctionRegistry())
                .normalizationForm(rdfTermGeneratorConfig.getNormalizationForm())
                .rdfTermGeneratorFactory(this)
                .build();
    }

    private List<MappedValue<Value>> generateTerms(
            List<Object> values,
            TermMap termMap,
            IRI mappedDatatype,
            List<IRI> overrideDatatypes,
            List<String> languageTags) {
        return switch (termMap.getTermType()) {
            case IRI, URI, UNSAFE_IRI ->
                values.stream()
                        .map(value -> CanonicalRdfLexicalForm.get().apply(value, mappedDatatype))
                        .map(lexicalForm -> generateIriTerm(lexicalForm, termMap))
                        .toList();
            case BLANK_NODE ->
                values.stream()
                        .map(value -> CanonicalRdfLexicalForm.get().apply(value, mappedDatatype))
                        .map(lexicalForm -> generateBNodeTerm(lexicalForm, termMap))
                        .toList();
            case LITERAL -> generateLiteralTerms(values, termMap, mappedDatatype, overrideDatatypes, languageTags);
        };
    }

    private MappedValue<Value> generateIriTerm(String lexicalForm, TermMap termMap) {
        if (termMap.getTermType() == TermType.UNSAFE_IRI) {
            return generateUnsafeIriTerm(lexicalForm, termMap);
        }

        if (RdfValues.isValidIri(lexicalForm)) {
            return RdfMappedValue.of(valueFactory.createIRI(lexicalForm), termMap.getTargets());
        }

        String iri = rdfTermGeneratorConfig.getBaseIri().stringValue() + lexicalForm;
        if (RdfValues.isValidIri(iri)) {
            return RdfMappedValue.of(valueFactory.createIRI(iri), termMap.getTargets());
        }

        throw new TermGeneratorFactoryException(String.format(
                "Could not generate a valid iri from term lexical form [%s] as-is, or prefixed with base iri [%s]",
                lexicalForm, rdfTermGeneratorConfig.getBaseIri()));
    }

    private MappedValue<Value> generateUnsafeIriTerm(String lexicalForm, TermMap termMap) {
        // SIMPLE_VALUE_FACTORY is used intentionally to bypass IRI validation, since rml:UnsafeIRI
        // produces IRIs without percent-encoding that may contain characters invalid per RFC 3987.
        if (lexicalForm.contains(":")) {
            return RdfMappedValue.of(SIMPLE_VALUE_FACTORY.createIRI(lexicalForm), termMap.getTargets());
        }
        String unsafeIri = rdfTermGeneratorConfig.getBaseIri().stringValue() + lexicalForm;
        return RdfMappedValue.of(SIMPLE_VALUE_FACTORY.createIRI(unsafeIri), termMap.getTargets());
    }

    private MappedValue<Value> generateBNodeTerm(String lexicalForm, TermMap termMap) {
        String id = createValidBNodeId(lexicalForm);
        return RdfMappedValue.of(valueFactory.createBNode(id), termMap.getTargets());
    }

    private String createValidBNodeId(String lexicalForm) {
        return lexicalForm.replaceAll("[^a-zA-Z_0-9-]+", "");
    }

    private List<MappedValue<Value>> generateLiteralTerms(
            List<Object> values,
            TermMap termMap,
            IRI mappedDatatype,
            List<IRI> overrideDatatypes,
            List<String> languageTags) {
        if (!languageTags.isEmpty()) {
            return languageTags.stream()
                    .flatMap(languageTag -> values.stream()
                            .map(value -> CanonicalRdfLexicalForm.get().apply(value, mappedDatatype))
                            .map(lexicalForm -> valueFactory.createLiteral(lexicalForm, languageTag))
                            .map(Value.class::cast))
                    .map(value -> RdfMappedValue.of(value, termMap.getTargets()))
                    .toList();
        }

        if (!overrideDatatypes.isEmpty()) {
            return generateDatatypedLiterals(values, termMap, overrideDatatypes);
        }

        if (mappedDatatype != null) {
            return generateDatatypedLiterals(values, termMap, List.of(mappedDatatype));
        }

        return values.stream()
                .map(this::createNativeTypedLiteral)
                .map(value -> RdfMappedValue.of(value, termMap.getTargets()))
                .toList();
    }

    /**
     * Creates a literal that preserves the Java type of the value. For example, an {@link Integer}
     * value produces a {@code xsd:integer} literal rather than a plain string literal. This is
     * important for function execution results where the Java return type carries datatype info.
     */
    private Value createNativeTypedLiteral(Object value) {
        if (value instanceof Integer intVal) {
            return valueFactory.createLiteral(intVal.toString(), XSD.INTEGER);
        }
        if (value instanceof Long longVal) {
            return valueFactory.createLiteral(longVal.toString(), XSD.INTEGER);
        }
        if (value instanceof Double doubleVal) {
            return valueFactory.createLiteral(doubleVal);
        }
        if (value instanceof Float floatVal) {
            return valueFactory.createLiteral(floatVal);
        }
        if (value instanceof Boolean boolVal) {
            return valueFactory.createLiteral(boolVal);
        }
        return valueFactory.createLiteral(Objects.toString(value));
    }

    private List<MappedValue<Value>> generateDatatypedLiterals(
            List<Object> values, TermMap termMap, List<IRI> datatypes) {
        return datatypes.stream()
                .flatMap(datatype -> values.stream()
                        .map(value -> CanonicalRdfLexicalForm.get().apply(value, datatype))
                        .map(lexicalForm -> (Value) valueFactory.createLiteral(lexicalForm, datatype)))
                .map(value -> RdfMappedValue.of(value, termMap.getTargets()))
                .toList();
    }
}
