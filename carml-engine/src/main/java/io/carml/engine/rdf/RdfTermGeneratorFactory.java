package io.carml.engine.rdf;

import static io.carml.model.TermType.BLANK_NODE;
import static io.carml.model.TermType.LITERAL;
import static io.carml.util.LogUtil.exception;

import io.carml.engine.TermGenerator;
import io.carml.engine.TermGeneratorFactory;
import io.carml.engine.TermGeneratorFactoryException;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.DatatypeMap;
import io.carml.model.ExpressionMap;
import io.carml.model.GraphMap;
import io.carml.model.LanguageMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.SubjectMap;
import io.carml.model.TermMap;
import io.carml.model.TermType;
import io.carml.util.IriSafeMaker;
import io.carml.util.RdfValues;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
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
        validateTermType(subjectMap, Set.of(BLANK_NODE, TermType.IRI));
        return (TermGenerator<Resource>) createTermGenerator(subjectMap, subjectMap.getTermType(), Set.of(IRI.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<IRI> getPredicateGenerator(PredicateMap predicateMap) {
        validateTermType(predicateMap, Set.of(TermType.IRI));
        return (TermGenerator<IRI>) createTermGenerator(predicateMap, predicateMap.getTermType(), Set.of(IRI.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<Value> getObjectGenerator(ObjectMap objectMap) {
        validateTermType(objectMap, Set.of(TermType.IRI, BLANK_NODE, LITERAL));

        var generateOverrideDatatypes =
                objectMap.getDatatypeMap() == null ? null : getDatatypeGenerator(objectMap.getDatatypeMap());

        return (TermGenerator<Value>) createTermGenerator(
                objectMap,
                objectMap.getTermType(),
                Set.of(IRI.class, Literal.class),
                generateOverrideDatatypes,
                objectMap.getLanguageMap());
    }

    @Override
    @SuppressWarnings("unchecked")
    public TermGenerator<Resource> getGraphGenerator(GraphMap graphMap) {
        validateTermType(graphMap, Set.of(TermType.IRI));
        return (TermGenerator<Resource>) createTermGenerator(graphMap, graphMap.getTermType(), Set.of(IRI.class));
    }

    @SuppressWarnings("unchecked")
    private TermGenerator<IRI> getDatatypeGenerator(DatatypeMap map) {
        return (TermGenerator<IRI>) createTermGenerator(map, TermType.IRI, Set.of(IRI.class));
    }

    private TermGenerator<? extends Value> createTermGenerator(
            ExpressionMap expressionMap, TermType termType, Set<Class<? extends Value>> allowedConstantTypes) {
        return createTermGenerator(expressionMap, termType, allowedConstantTypes, null, null);
    }

    private TermGenerator<? extends Value> createTermGenerator(
            ExpressionMap expressionMap,
            TermType termType,
            Set<Class<? extends Value>> allowedConstantTypes,
            TermGenerator<IRI> generateOverrideDatatypes,
            LanguageMap languageMap) {
        if (expressionMap.getConstant() != null) {
            return getConstantGenerator(expressionMap, allowedConstantTypes);
        } else if (expressionMap.getReference() != null) {
            return getReferenceGenerator(expressionMap, termType, generateOverrideDatatypes, languageMap);
        } else if (expressionMap.getTemplate() != null) {
            return getTemplateGenerator(expressionMap, termType, generateOverrideDatatypes, languageMap);
        } else if (expressionMap.getFunctionValue() != null) {
            return getFunctionValueGenerator(expressionMap, termType, generateOverrideDatatypes, languageMap);
        } else {
            throw new TermGeneratorFactoryException(
                    String.format("No valid expression found in %s", exception(expressionMap)));
        }
    }

    private TermGenerator<? extends Value> getConstantGenerator(
            ExpressionMap expressionMap, Set<Class<? extends Value>> allowedConstantTypes) {
        Value constant = expressionMap.getConstant();
        if (allowedConstantTypes.stream().noneMatch(allowed -> allowed.isInstance(constant))) {
            throw new TermGeneratorFactoryException(String.format(
                    "encountered constant value of type %s, which is not allowed for this term map",
                    constant.getClass().getSimpleName()));
        }

        return (expressionEvaluation, datatypeMapper) -> RdfExpressionMapEvaluation.builder()
                .expressionMap(expressionMap)
                .build()
                .evaluate(Value.class);
    }

    private TermGenerator<? extends Value> getReferenceGenerator(
            ExpressionMap expressionMap,
            TermType termType,
            TermGenerator<IRI> generateOverrideDatatypes,
            LanguageMap languageMap) {
        var reference = expressionMap.getReference();

        return (expressionEvaluation, datatypeMapper) -> {
            var referenceValues = RdfExpressionMapEvaluation.builder()
                    .expressionMap(expressionMap)
                    .expressionEvaluation(expressionEvaluation)
                    .build()
                    .evaluate(Object.class);

            var mappedDatatype = datatypeMapper == null
                    ? null
                    : datatypeMapper.apply(reference).orElse(null);
            var overrideDatatypes =
                    getOverrideDatatypes(generateOverrideDatatypes, expressionEvaluation, datatypeMapper);
            var languageTags = getLanguageTags(languageMap, expressionEvaluation, datatypeMapper);

            return generateTerms(referenceValues, termType, mappedDatatype, overrideDatatypes, languageTags);
        };
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

    private TermGenerator<? extends Value> getTemplateGenerator(
            ExpressionMap expressionMap,
            TermType termType,
            TermGenerator<IRI> generateOverrideDatatypes,
            LanguageMap languageMap) {
        UnaryOperator<String> valueTransformingFunction =
                termType == TermType.IRI ? makeIriSafe : UnaryOperator.identity();

        return (expressionEvaluation, datatypeMapper) -> {
            var templateValues = RdfExpressionMapEvaluation.builder()
                    .expressionMap(expressionMap)
                    .expressionEvaluation(expressionEvaluation)
                    .datatypeMapper(datatypeMapper)
                    .templateReferenceValueTransformingFunction(valueTransformingFunction)
                    .build()
                    .evaluate(Object.class);

            var overrideDatatypes =
                    getOverrideDatatypes(generateOverrideDatatypes, expressionEvaluation, datatypeMapper);
            var languageTags = getLanguageTags(languageMap, expressionEvaluation, datatypeMapper);

            return generateTerms(templateValues, termType, null, overrideDatatypes, languageTags);
        };
    }

    private TermGenerator<? extends Value> getFunctionValueGenerator(
            ExpressionMap expressionMap,
            TermType termType,
            TermGenerator<IRI> generateOverrideDatatypes,
            LanguageMap languageMap) {
        return (expressionEvaluation, datatypeMapper) -> {
            throw new TermGeneratorFactoryException("FunctionValue is not supported yet");
        };
    }

    private List<IRI> getOverrideDatatypes(
            TermGenerator<IRI> generateOverrideDatatypes,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        return generateOverrideDatatypes != null
                ? generateOverrideDatatypes.apply(expressionEvaluation, datatypeMapper)
                : List.of();
    }

    private List<Value> generateTerms(
            List<Object> values,
            TermType termType,
            IRI mappedDatatype,
            List<IRI> overrideDatatypes,
            List<String> languageTags) {
        return switch (termType) {
            case IRI -> values.stream()
                    .map(value -> CanonicalRdfLexicalForm.get().apply(value, mappedDatatype))
                    .map(this::generateIriTerm)
                    .toList();
            case BLANK_NODE -> values.stream()
                    .map(value -> CanonicalRdfLexicalForm.get().apply(value, mappedDatatype))
                    .map(this::generateBNodeTerm)
                    .toList();
            case LITERAL -> generateLiteralTerms(values, mappedDatatype, overrideDatatypes, languageTags);
        };
    }

    private Value generateIriTerm(String lexicalForm) {
        if (RdfValues.isValidIri(lexicalForm)) {
            return valueFactory.createIRI(lexicalForm);
        }

        String iri = rdfTermGeneratorConfig.getBaseIri().stringValue() + lexicalForm;
        if (RdfValues.isValidIri(iri)) {
            return valueFactory.createIRI(iri);
        }

        throw new TermGeneratorFactoryException(String.format(
                "Could not generate a valid iri from term lexical form [%s] as-is, or prefixed with base iri [%s]",
                lexicalForm, rdfTermGeneratorConfig.getBaseIri()));
    }

    private Value generateBNodeTerm(String lexicalForm) {
        String id = createValidBNodeId(lexicalForm);
        return valueFactory.createBNode(id);
    }

    private String createValidBNodeId(String lexicalForm) {
        return lexicalForm.replaceAll("[^a-zA-Z_0-9-]+", "");
    }

    private List<Value> generateLiteralTerms(
            List<Object> values, IRI mappedDatatype, List<IRI> overrideDatatypes, List<String> languageTags) {
        if (!languageTags.isEmpty()) {
            return languageTags.stream()
                    .flatMap(languageTag -> values.stream()
                            .map(value -> CanonicalRdfLexicalForm.get().apply(value, mappedDatatype))
                            .map(lexicalForm -> valueFactory.createLiteral(lexicalForm, languageTag))
                            .map(Value.class::cast))
                    .toList();
        }

        if (!overrideDatatypes.isEmpty()) {
            return generateDatatypedLiterals(values, overrideDatatypes);
        }

        if (mappedDatatype != null) {
            return generateDatatypedLiterals(values, List.of(mappedDatatype));
        }

        return values.stream()
                .map(Objects::toString)
                .map(valueFactory::createLiteral)
                .map(Value.class::cast)
                .toList();
    }

    private List<Value> generateDatatypedLiterals(List<Object> values, List<IRI> datatypes) {
        return datatypes.stream()
                .flatMap(datatype -> values.stream()
                        .map(value -> CanonicalRdfLexicalForm.get().apply(value, datatype))
                        .map(lexicalForm -> (Value) valueFactory.createLiteral(lexicalForm, datatype)))
                .toList();
    }
}
