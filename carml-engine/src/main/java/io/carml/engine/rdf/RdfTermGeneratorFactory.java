package io.carml.engine.rdf;

import static io.carml.engine.rdf.RdfPredicateObjectMapper.createObjectMapGenerators;
import static io.carml.engine.rdf.RdfPredicateObjectMapper.createPredicateGenerators;
import static io.carml.util.Models.streamCartesianProductStatements;

import io.carml.engine.ExpressionEvaluation;
import io.carml.engine.GetTemplateValue;
import io.carml.engine.TermGenerator;
import io.carml.engine.TermGeneratorFactory;
import io.carml.engine.TermGeneratorFactoryException;
import io.carml.engine.function.ExecuteFunction;
import io.carml.engine.template.Template;
import io.carml.engine.template.TemplateParser;
import io.carml.model.DatatypeMap;
import io.carml.model.ExpressionMap;
import io.carml.model.GraphMap;
import io.carml.model.LanguageMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TermMap;
import io.carml.model.TermType;
import io.carml.model.TriplesMap;
import io.carml.util.IriSafeMaker;
import io.carml.util.RdfValues;
import io.carml.vocab.Rdf;
import java.text.Normalizer;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.eclipse.rdf4j.common.net.ParsedIRI;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.util.Models;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("java:S1135")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfTermGeneratorFactory implements TermGeneratorFactory<Value> {

  // TODO cache results of evaluated expressions?

  private static final String RML_BASE_IRI = "http://example.com/base/";

  private static final Logger LOG = LoggerFactory.getLogger(RdfTermGeneratorFactory.class);

  private final RdfMapperOptions mapperOptions;

  private final ValueFactory valueFactory;

  private final UnaryOperator<String> makeIriSafe;

  private final TemplateParser templateParser;

  public static RdfTermGeneratorFactory of(RdfMapperOptions mapperOptions, TemplateParser templateParser) {
    return new RdfTermGeneratorFactory(mapperOptions, mapperOptions.getValueFactory(),
        IriSafeMaker.create(mapperOptions.getNormalizationForm(), mapperOptions.isIriUpperCasePercentEncoding()),
        templateParser);
  }

  @Override
  @SuppressWarnings("unchecked")
  public TermGenerator<Resource> getSubjectGenerator(SubjectMap map) {
    return (TermGenerator<Resource>) getGenerator(map, Set.of(TermType.BLANK_NODE, TermType.IRI), Set.of(IRI.class));
  }

  @Override
  @SuppressWarnings("unchecked")
  public TermGenerator<IRI> getPredicateGenerator(PredicateMap map) {
    return (TermGenerator<IRI>) getGenerator(map, Set.of(TermType.IRI), Set.of(IRI.class));
  }

  @Override
  @SuppressWarnings("unchecked")
  public TermGenerator<Value> getObjectGenerator(ObjectMap map) {
    return (TermGenerator<Value>) getGenerator(map, Set.of(TermType.IRI, TermType.BLANK_NODE, TermType.LITERAL),
        Set.of(IRI.class, Literal.class));
  }

  @Override
  @SuppressWarnings("unchecked")
  public TermGenerator<Resource> getGraphGenerator(GraphMap map) {
    return (TermGenerator<Resource>) getGenerator(map, Set.of(TermType.IRI), Set.of(IRI.class));
  }

  @SuppressWarnings("unchecked")
  private TermGenerator<IRI> getDatatypeGenerator(DatatypeMap map) {
    return (TermGenerator<IRI>) getGenerator(map, Set.of(TermType.IRI), Set.of(IRI.class));
  }

  @SuppressWarnings("unchecked")
  private TermGenerator<Literal> getLanguageGenerator(LanguageMap map) {
    return (TermGenerator<Literal>) getGenerator(map, Set.of(TermType.LITERAL), Set.of(Literal.class));
  }

  private TermGenerator<? extends Value> getGenerator(ExpressionMap map, Set<TermType> allowedTermTypes,
      Set<Class<? extends Value>> allowedConstantTypes) {
    List<TermGenerator<? extends Value>> generators = Stream.<Supplier<Optional<TermGenerator<? extends Value>>>>of(

        // constant
        () -> getConstantGenerator(map, allowedConstantTypes),

        // reference
        () -> getReferenceGenerator(map, allowedTermTypes),

        // template
        () -> getTemplateGenerator(map, allowedTermTypes),

        // functionValue
        () -> getFunctionValueGenerator(map, allowedTermTypes)

    )
        .map(Supplier::get)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());

    if (generators.isEmpty()) {
      throw new TermGeneratorFactoryException(String
          .format("No constant, reference, template or function value found for term map [%s]", map.getResourceName()));
    }
    if (generators.size() > 1) {
      throw new TermGeneratorFactoryException(
          String.format("%s value generators were created for term map [%s], where only 1 is expected.",
              generators.size(), map.getResourceName()));
    }
    return generators.get(0);
  }

  private TermGenerator<Value> getGenerator(ExpressionMap termMap,
      Function<ExpressionEvaluation, Optional<Object>> getValue, Set<TermType> allowedTermTypes, TermType termType) {

    Function<Function<String, ? extends Value>, TermGenerator<Value>> createGenerator =
        generateTerm -> expressionEvaluation -> generateValues(getValue, expressionEvaluation, generateTerm);

    if (!allowedTermTypes.contains(termType)) {
      throw new TermGeneratorFactoryException(
          String.format("encountered disallowed term type [%s]%nin TermMap:%n%s%n%n allowed TermTypes: %s", termType,
              termMap, allowedTermTypes));
    }

    switch (termType) {

      case IRI:
        return createGenerator.apply(this::generateIriTerm);

      case BLANK_NODE:
        return createGenerator.apply(this::generateBNodeTerm);

      case LITERAL:

        // term map is assumed to be an object map if it has term type literal
        ObjectMap objectMap = (ObjectMap) termMap;

        if (objectMap.getLanguageMap() != null) {
          return getLanguageTaggedLiteralGenerator(objectMap, getValue);
        }

        if (objectMap.getDatatypeMap() != null) {
          return getDatatypedLiteralGenerator(objectMap, getValue);
        }

        // f.createLiteral(label, datatype) // TODO infer datatype, see
        // https://www.w3.org/TR/r2rml/#generated-rdf-term - f.e. xsd:integer for Integer instances
        return createGenerator.apply(valueFactory::createLiteral);

      default:
        throw new TermGeneratorFactoryException(
            String.format("unknown term type [%s]%nin TermMap:%s", termType, termMap));

    }
  }

  private List<Value> generateValues(Function<ExpressionEvaluation, Optional<Object>> getValue,
      ExpressionEvaluation expressionEvaluation, Function<String, ? extends Value> generateTerm) {
    Optional<Object> referenceValue = getValue.apply(expressionEvaluation);
    if (LOG.isTraceEnabled()) {
      LOG.trace("with result: {}", referenceValue.orElse("null"));
    }

    return referenceValue.map(value -> unpackEvaluatedExpression(value, generateTerm))
        .orElse(List.of());
  }

  private TermGenerator<Value> getDatatypedLiteralGenerator(ObjectMap objectMap,
      Function<ExpressionEvaluation, Optional<Object>> getLabelValue) {

    return expressionEvaluation -> {
      // determine label values
      List<Value> labels = generateValues(getLabelValue, expressionEvaluation, valueFactory::createLiteral);

      // determine datatypes by creating a nested term generator
      List<IRI> datatypes = getDatatypeGenerator(objectMap.getDatatypeMap()).apply(expressionEvaluation);

      // return literals for all combinations of label and datatype
      return labels.stream()
          .map(Value::stringValue)
          .flatMap(label -> datatypes.stream()
              .map(datatype -> valueFactory.createLiteral(label, datatype)))
          .collect(Collectors.toUnmodifiableList());
    };
  }

  private TermGenerator<Value> getLanguageTaggedLiteralGenerator(ObjectMap objectMap,
      Function<ExpressionEvaluation, Optional<Object>> getLabelValue) {

    return expressionEvaluation -> {
      // determine label values
      List<Value> labels = generateValues(getLabelValue, expressionEvaluation, valueFactory::createLiteral);

      // determine languages by creating a nested term generator
      // TODO languages arent really literals, but that would require some refactoring
      List<Literal> languages = getLanguageGenerator(objectMap.getLanguageMap()).apply(expressionEvaluation);

      // return literals for all combinations of label and datatype
      return labels.stream()
          .map(Value::stringValue)
          .flatMap(label -> languages.stream()
              .map(Literal::getLabel)
              .filter(language -> {
                if (!Literals.isValidLanguageTag(language)) {
                  throw new TermGeneratorFactoryException(
                      String.format("Invalid lang tag '%s' used in object map %n%s", language, objectMap));
                }
                return true;
              })
              .map(language -> valueFactory.createLiteral(label, language)))
          .collect(Collectors.toUnmodifiableList());
    };
  }

  @Override
  public Optional<TermGenerator<? extends Value>> getConstantGenerator(ExpressionMap map,
      Set<Class<? extends Value>> allowedConstantTypes) {
    Value constant = map.getConstant();
    if (constant == null) {
      return Optional.empty();
    }
    if (allowedConstantTypes.stream()
        .noneMatch(c -> c.isInstance(constant))) {
      throw new TermGeneratorFactoryException(
          "encountered constant value of type " + constant.getClass() + ", which is not allowed for this term map");
    }
    List<Value> constants = List.of(constant);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Generated constant values: {}", constants);
    }

    return Optional.of(e -> constants);
  }

  @Override
  public Optional<TermGenerator<? extends Value>> getReferenceGenerator(ExpressionMap map,
      Set<TermType> allowedTermTypes) {

    String reference = map.getReference();
    if (reference == null) {
      return Optional.empty();
    }

    Function<ExpressionEvaluation, Optional<Object>> getValue =
        expressionEvaluation -> expressionEvaluation.apply(reference);

    return Optional.of(getGenerator(map, getValue, allowedTermTypes, determineTermType(map)));
  }

  @Override
  public Optional<TermGenerator<? extends Value>> getTemplateGenerator(ExpressionMap map,
      Set<TermType> allowedTermTypes) {

    String templateStr = map.getTemplate();
    if (templateStr == null) {
      return Optional.empty();
    }

    Template template = templateParser.parse(templateStr);

    TermType termType = determineTermType(map);

    // for IRI term types, make template values 'IRI-safe'.
    // otherwise, do not transform template values.
    UnaryOperator<String> transformValue = termType == TermType.IRI ? makeIriSafe : v -> v;

    Function<ExpressionEvaluation, Optional<Object>> getValue =
        new GetTemplateValue(template, template.getExpressions(), transformValue, this::createNaturalRdfLexicalForm);

    return Optional.of(getGenerator(map, getValue, allowedTermTypes, termType));
  }

  @Override
  public Optional<TermGenerator<? extends Value>> getFunctionValueGenerator(ExpressionMap expressionMap,
      Set<TermType> allowedTermTypes) {
    var executionMap = expressionMap.getFunctionValue();
    if (executionMap == null) {
      return Optional.empty();
    }

    Function<ExpressionEvaluation, Optional<Object>> getValue =
        expressionEvaluation -> mapFunctionExecution(expressionEvaluation, expressionMap, executionMap);

    return Optional.of(getGenerator(expressionMap, getValue, allowedTermTypes, determineTermType(expressionMap)));
  }

  private Optional<Object> mapFunctionExecution(ExpressionEvaluation expressionEvaluation, ExpressionMap expressionMap,
      TriplesMap executionMap) {
    Resource functionExecution = valueFactory.createBNode();

    var executionStatements = executionMap.getPredicateObjectMaps()
        .stream()
        .flatMap(pom -> getFunctionPredicateObjectMapModel(functionExecution, executionMap, pom, expressionEvaluation))
        .collect(new ModelCollector());

    var termType = determineTermType(expressionMap);

    // for IRI term types, make values valid IRIs.
    UnaryOperator<Object> returnValueAdapter = termType == TermType.IRI ? this::iriEncodeResult : v -> v;

    return mapExecution(executionStatements, returnValueAdapter);
  }

  private Stream<Statement> getFunctionPredicateObjectMapModel(Resource functionExecution, TriplesMap executionMap,
      PredicateObjectMap pom, ExpressionEvaluation expressionEvaluation) {
    var predicateGenerators = createPredicateGenerators(pom, executionMap, this);
    var objectGenerators = createObjectMapGenerators(pom.getObjectMaps(), executionMap, this);

    Set<IRI> predicates = predicateGenerators.stream()
        .map(g -> g.apply(expressionEvaluation))
        .flatMap(List::stream)
        .collect(Collectors.toUnmodifiableSet());

    if (predicates.isEmpty()) {
      return Stream.empty();
    }

    Set<Value> objects = objectGenerators.map(g -> g.apply(expressionEvaluation))
        .flatMap(List::stream)
        .collect(Collectors.toUnmodifiableSet());

    if (objects.isEmpty()) {
      return Stream.empty();
    }

    return streamCartesianProductStatements(Set.of(functionExecution), predicates, objects, Set.of());
  }

  private Optional<Object> mapExecution(Model executionStatements, UnaryOperator<Object> returnValueAdapter) {
    Optional<Resource> optionalExecution = Models.subject(executionStatements);

    return optionalExecution.map(execution -> {
      IRI functionIri = getFunctionIri(execution, executionStatements);
      ExecuteFunction function = mapperOptions.getFunctions()
          .getFunction(functionIri)
          .orElseThrow(
              () -> new TermGeneratorFactoryException("no function registered for function IRI [" + functionIri + "]"));

      return function.execute(executionStatements, execution, returnValueAdapter);
    });
  }

  private Object iriEncodeResult(Object result) {
    if (result instanceof Collection<?>) {
      return ((Collection<?>) result).stream()
          .map(this::encodeAsIri)
          .collect(Collectors.toUnmodifiableList());
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
    iriValue = Normalizer.normalize(iriValue, mapperOptions.getNormalizationForm());

    return ParsedIRI.create(iriValue)
        .toString();
  }

  private IRI getFunctionIri(Resource execution, Model model) {
    return Models.objectIRI(model.filter(execution, Rdf.Fno.executes, null))
        .orElseGet(() -> Models.objectIRI(model.filter(execution, Rdf.Fno.old_executes, null))
            .orElseThrow(
                () -> new TermGeneratorFactoryException("function execution does not have fno:executes value")));
  }

  private TermType determineTermType(ExpressionMap map) {
    if (map instanceof DatatypeMap) {
      return TermType.IRI;
    } else if (map instanceof LanguageMap) {
      return TermType.LITERAL;
    } else if (map instanceof TermMap) {
      TermMap termMap = (TermMap) map;

      TermType termType = termMap.getTermType();
      if (termType != null) {
        return termType;
      }

      if (map instanceof ObjectMap) {
        ObjectMap objectMap = (ObjectMap) map;
        if (isReferenceTermMap(termMap) || objectMap.getLanguageMap() != null || objectMap.getDatatypeMap() != null) {
          return TermType.LITERAL;
        }
      }

      return TermType.IRI;
    } else {
      throw new IllegalStateException(String.format("Unknown expression map type %s for %s", map.getClass()
          .getSimpleName(), map));
    }
  }

  private boolean isReferenceTermMap(TermMap map) {
    return map.getConstant() == null && map.getReference() != null;
  }

  private List<Value> unpackEvaluatedExpression(Object result, Function<String, ? extends Value> generateTerm) {
    if (result instanceof Collection<?>) {
      return ((Collection<?>) result).stream()
          .map(i -> generateTerm.apply(createNaturalRdfLexicalForm(i)))
          .collect(Collectors.toUnmodifiableList());
    }

    Value value = generateTerm.apply(createNaturalRdfLexicalForm(result));

    return value == null ? List.of() : List.of(value);
  }

  private IRI generateIriTerm(String lexicalForm) {
    if (RdfValues.isValidIri(lexicalForm)) {
      return valueFactory.createIRI(lexicalForm);
    }

    String iri = RML_BASE_IRI + lexicalForm;
    if (RdfValues.isValidIri(iri)) {
      return valueFactory.createIRI(iri);
    }

    throw new TermGeneratorFactoryException(String.format(
        "Could not generate a valid iri from term lexical form [%s] as-is, or prefixed with base iri [%s]", lexicalForm,
        RML_BASE_IRI));
  }

  private BNode generateBNodeTerm(String lexicalForm) {
    String id = createValidBNodeId(lexicalForm);
    return valueFactory.createBNode(id);
  }

  private String createValidBNodeId(String lexicalForm) {
    return lexicalForm.replaceAll("[^a-zA-Z_0-9-]+", "");
  }

  private String createNaturalRdfLexicalForm(Object value) {
    // TODO https://www.w3.org/TR/r2rml/#dfn-natural-rdf-literal
    return value.toString();
  }

}
