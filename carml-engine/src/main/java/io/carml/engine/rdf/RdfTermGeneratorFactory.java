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
import io.carml.model.GatherMap;
import io.carml.model.GraphMap;
import io.carml.model.LanguageMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.SubjectMap;
import io.carml.model.TermMap;
import io.carml.model.TermType;
import io.carml.util.IriSafeMaker;
import io.carml.util.RdfValues;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfTermGeneratorFactory implements TermGeneratorFactory<Value> {

    private final RdfTermGeneratorConfig rdfTermGeneratorConfig;

    private final ValueFactory valueFactory;

    private final UnaryOperator<String> makeIriSafe;

    public static RdfTermGeneratorFactory of(RdfTermGeneratorConfig rdfTermGeneratorConfig) {
        return new RdfTermGeneratorFactory(
                rdfTermGeneratorConfig,
                rdfTermGeneratorConfig.getValueFactory(),
                IriSafeMaker.create(
                        rdfTermGeneratorConfig.getNormalizationForm(),
                        rdfTermGeneratorConfig.isIriUpperCasePercentEncoding()));
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
            return (expressionEvaluation, datatypeMapper) -> Set.of(RdfMappedValue.of(valueFactory.createBNode()));
        }

        validateTermType(subjectMap, Set.of(BLANK_NODE, TermType.IRI));
        return (TermGenerator<Resource>) createTermGenerator(subjectMap, Set.of(IRI.class));
    }

    private boolean isBlankSubjectMap(SubjectMap subjectMap) {
        return subjectMap == null
                || (subjectMap.getReferenceExpressionSet().isEmpty()
                        && subjectMap.getGathers().isEmpty());
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<IRI> getPredicateGenerator(PredicateMap predicateMap) {
        validateTermType(predicateMap, Set.of(TermType.IRI));
        return (TermGenerator<IRI>) createTermGenerator(predicateMap, Set.of(IRI.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<Value> getObjectGenerator(ObjectMap objectMap) {
        validateTermType(objectMap, Set.of(TermType.IRI, BLANK_NODE, LITERAL));

        var generateOverrideDatatypes =
                objectMap.getDatatypeMap() == null ? null : getDatatypeGenerator(objectMap.getDatatypeMap());

        return (TermGenerator<Value>) createTermGenerator(
                objectMap, Set.of(IRI.class, Literal.class), generateOverrideDatatypes, objectMap.getLanguageMap());
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<Resource> getGraphGenerator(GraphMap graphMap) {
        validateTermType(graphMap, Set.of(TermType.IRI));
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
        } else if (termMap.getFunctionValue() != null) {
            return getFunctionValueGenerator(termMap, generateOverrideDatatypes, languageMap);
        } else {
            throw new TermGeneratorFactoryException(
                    String.format("No valid expression found in %s", exception(termMap)));
        }
    }

    private TermGenerator<? extends Value> getGatherMapGenerater(GatherMap gatherMap) {
        return (expressionEvaluation, datatypeMapper) -> RdfListOrContainerGenerator.of(gatherMap, valueFactory, this)
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

        return (expressionEvaluation, datatypeMapper) -> {
            var values = RdfExpressionMapEvaluation.builder()
                    .expressionMap(termMap)
                    .build()
                    .evaluate(Value.class);

            return values.stream()
                    .map(value -> RdfMappedValue.of(value, termMap.getTargets()))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        };
    }

    private TermGenerator<? extends Value> getReferenceGenerator(
            TermMap termMap, TermGenerator<? extends Value> generateOverrideDatatypes, LanguageMap languageMap) {
        var reference = termMap.getReference();

        return (expressionEvaluation, datatypeMapper) -> {
            var referenceValues = RdfExpressionMapEvaluation.builder()
                    .expressionMap(termMap)
                    .expressionEvaluation(expressionEvaluation)
                    .build()
                    .evaluate(Object.class);

            var mappedDatatype = datatypeMapper == null
                    ? null
                    : datatypeMapper.apply(reference).orElse(null);
            var overrideDatatypes =
                    getOverrideDatatypes(generateOverrideDatatypes, expressionEvaluation, datatypeMapper);
            var languageTags = getLanguageTags(languageMap, expressionEvaluation, datatypeMapper);

            return generateTerms(referenceValues, termMap, mappedDatatype, overrideDatatypes, languageTags);
        };
    }

    private TermGenerator<? extends Value> getTemplateGenerator(
            TermMap termMap, TermGenerator<? extends Value> generateOverrideDatatypes, LanguageMap languageMap) {
        UnaryOperator<String> valueTransformingFunction =
                termMap.getTermType() == TermType.IRI ? makeIriSafe : UnaryOperator.identity();

        return (expressionEvaluation, datatypeMapper) -> {
            var templateValues = RdfExpressionMapEvaluation.builder()
                    .expressionMap(termMap)
                    .expressionEvaluation(expressionEvaluation)
                    .datatypeMapper(datatypeMapper)
                    .templateReferenceValueTransformingFunction(valueTransformingFunction)
                    .build()
                    .evaluate(Object.class);

            var overrideDatatypes =
                    getOverrideDatatypes(generateOverrideDatatypes, expressionEvaluation, datatypeMapper);
            var languageTags = getLanguageTags(languageMap, expressionEvaluation, datatypeMapper);

            return generateTerms(templateValues, termMap, null, overrideDatatypes, languageTags);
        };
    }

    private TermGenerator<? extends Value> getFunctionValueGenerator(
            TermMap termMap, TermGenerator<? extends Value> generateOverrideDatatypes, LanguageMap languageMap) {
        return (expressionEvaluation, datatypeMapper) -> {
            throw new TermGeneratorFactoryException("FunctionValue is not supported yet");
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
            LanguageMap languageMap, ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        if (languageMap == null) {
            return List.of();
        }

        return RdfExpressionMapEvaluation.builder()
                .expressionMap(languageMap)
                .expressionEvaluation(expressionEvaluation)
                .datatypeMapper(datatypeMapper)
                .build()
                .evaluate(String.class);
    }

    private Set<MappedValue<Value>> generateTerms(
            List<Object> values,
            TermMap termMap,
            IRI mappedDatatype,
            List<IRI> overrideDatatypes,
            List<String> languageTags) {
        return switch (termMap.getTermType()) {
            case IRI -> values.stream()
                    .map(value -> CanonicalRdfLexicalForm.get().apply(value, mappedDatatype))
                    .map(lexicalForm -> generateIriTerm(lexicalForm, termMap))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            case BLANK_NODE -> values.stream()
                    .map(value -> CanonicalRdfLexicalForm.get().apply(value, mappedDatatype))
                    .map(lexicalForm -> generateBNodeTerm(lexicalForm, termMap))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            case LITERAL -> generateLiteralTerms(values, termMap, mappedDatatype, overrideDatatypes, languageTags);
        };
    }

    private MappedValue<Value> generateIriTerm(String lexicalForm, TermMap termMap) {
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

    private MappedValue<Value> generateBNodeTerm(String lexicalForm, TermMap termMap) {
        String id = createValidBNodeId(lexicalForm);
        return RdfMappedValue.of(valueFactory.createBNode(id), termMap.getTargets());
    }

    private String createValidBNodeId(String lexicalForm) {
        return lexicalForm.replaceAll("[^a-zA-Z_0-9-]+", "");
    }

    private Set<MappedValue<Value>> generateLiteralTerms(
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
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        if (!overrideDatatypes.isEmpty()) {
            return generateDatatypedLiterals(values, termMap, overrideDatatypes);
        }

        if (mappedDatatype != null) {
            return generateDatatypedLiterals(values, termMap, List.of(mappedDatatype));
        }

        return values.stream()
                .map(Objects::toString)
                .map(valueFactory::createLiteral)
                .map(Value.class::cast)
                .map(value -> RdfMappedValue.of(value, termMap.getTargets()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Set<MappedValue<Value>> generateDatatypedLiterals(
            List<Object> values, TermMap termMap, List<IRI> datatypes) {
        return datatypes.stream()
                .flatMap(datatype -> values.stream()
                        .map(value -> CanonicalRdfLexicalForm.get().apply(value, datatype))
                        .map(lexicalForm -> (Value) valueFactory.createLiteral(lexicalForm, datatype)))
                .map(value -> RdfMappedValue.of(value, termMap.getTargets()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
