package com.taxonic.carml.engine.rdf;

import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.GetTemplateValue;
import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.TermGeneratorFactory;
import com.taxonic.carml.engine.TermGeneratorFactoryException;
import com.taxonic.carml.engine.function.ExecuteFunction;
import com.taxonic.carml.engine.reactivedev.join.MapDbParentSideJoinConditionStoreProvider;
import com.taxonic.carml.engine.template.Template;
import com.taxonic.carml.engine.template.TemplateParser;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TermMap;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.IriSafeMaker;
import com.taxonic.carml.util.RdfValues;
import com.taxonic.carml.vocab.Rdf;
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
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.util.Models;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfTermGeneratorFactory implements TermGeneratorFactory<Value> {

  // TODO cache results of evaluated expressions?

  private static final String RML_BASE_IRI = "http://example.com/base/";

  private static final Logger LOG = LoggerFactory.getLogger(RdfTermGeneratorFactory.class);

  private final ValueFactory valueFactory;

  private final String baseIri;

  private final RdfMapperOptions mapperOptions;

  private final UnaryOperator<String> makeIriSafe;

  private final TemplateParser templateParser;

  public static RdfTermGeneratorFactory of(ValueFactory valueFactory, RdfMapperOptions mapperOptions,
      TemplateParser templateParser) {
    return new RdfTermGeneratorFactory(valueFactory, RML_BASE_IRI, mapperOptions,
        IriSafeMaker.create(mapperOptions.getNormalizationForm(), mapperOptions.isIriUpperCasePercentEncoding()),
        templateParser);
  }

  @SuppressWarnings("unchecked")
  public TermGenerator<Resource> getSubjectGenerator(SubjectMap map) {
    return (TermGenerator<Resource>) getGenerator(map, Set.of(TermType.BLANK_NODE, TermType.IRI), Set.of(IRI.class));
  }

  @SuppressWarnings("unchecked")
  public TermGenerator<IRI> getPredicateGenerator(PredicateMap map) {
    return (TermGenerator<IRI>) getGenerator(map, Set.of(TermType.IRI), Set.of(IRI.class));
  }

  @SuppressWarnings("unchecked")
  public TermGenerator<Value> getObjectGenerator(ObjectMap map) {
    return (TermGenerator<Value>) getGenerator(map, Set.of(TermType.IRI, TermType.BLANK_NODE, TermType.LITERAL),
        Set.of(IRI.class, Literal.class));
  }

  @SuppressWarnings("unchecked")
  public TermGenerator<Resource> getGraphGenerator(GraphMap map) {
    return (TermGenerator<Resource>) getGenerator(map, Set.of(TermType.IRI), Set.of(IRI.class));
  }

  private TermGenerator<? extends Value> getGenerator(TermMap map, Set<TermType> allowedTermTypes,
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

  private TermGenerator<Value> getGenerator(TermMap termMap, Function<ExpressionEvaluation, Optional<Object>> getValue,
      Set<TermType> allowedTermTypes, TermType termType) {

    Function<Function<String, ? extends Value>, TermGenerator<Value>> createGenerator =
        generateTerm -> evaluateExpression -> {
          Optional<Object> referenceValue = getValue.apply(evaluateExpression);
          if (LOG.isTraceEnabled()) {
            LOG.trace("with result: {}", referenceValue.orElse("null"));
          }

          return referenceValue.map(value -> unpackEvaluatedExpression(value, generateTerm))
              .orElse(List.of());
        };

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

        String languageTag = objectMap.getLanguage();

        if (languageTag != null) {
          if (!Literals.isValidLanguageTag(languageTag)) {
            throw new TermGeneratorFactoryException(
                String.format("Invalid lang tag '%s' used in object map %n%s", languageTag, objectMap));
          }
          return createGenerator.apply(lexicalForm -> valueFactory.createLiteral(lexicalForm, languageTag));
        }

        IRI datatype = objectMap.getDatatype();
        if (datatype != null) {
          return createGenerator.apply(lexicalForm -> valueFactory.createLiteral(lexicalForm, datatype));
        }

        // f.createLiteral(label, datatype) // TODO infer datatype, see
        // https://www.w3.org/TR/r2rml/#generated-rdf-term - f.e. xsd:integer for Integer instances
        return createGenerator.apply(valueFactory::createLiteral);

      default:
        throw new TermGeneratorFactoryException(
            String.format("unknown term type [%s]%nin TermMap:%s", termType, termMap));

    }
  }

  public Optional<TermGenerator<? extends Value>> getConstantGenerator(TermMap map,
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

  public Optional<TermGenerator<? extends Value>> getReferenceGenerator(TermMap map, Set<TermType> allowedTermTypes) {

    String reference = map.getReference();
    if (reference == null) {
      return Optional.empty();
    }

    Function<ExpressionEvaluation, Optional<Object>> getValue =
        expressionEvaluation -> expressionEvaluation.apply(reference);

    return Optional.of(getGenerator(map, getValue, allowedTermTypes, determineTermType(map)));
  }

  public Optional<TermGenerator<? extends Value>> getTemplateGenerator(TermMap map, Set<TermType> allowedTermTypes) {

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

  public Optional<TermGenerator<? extends Value>> getFunctionValueGenerator(TermMap termMap,
      Set<TermType> allowedTermTypes) {

    TriplesMap executionMap = termMap.getFunctionValue();
    if (executionMap == null) {
      return Optional.empty();
    }

    // when 'executionMap' is evaluated, the generated triples
    // describe a fno:Execution instance, which we can then execute.

    // TODO check that executionMap has an identical logical source?

    // TODO: RefObjectMappers?? pass to TermGeneratorFactory?
    RdfTriplesMapper<?> executionTriplesMapper = RdfTriplesMapper.of(executionMap, Set.of(), Set.of(),
        a -> b -> Optional.empty(), RdfMappingContext.builder()
            .valueFactorySupplier(() -> valueFactory)
            .termGeneratorFactory(this)
            .build(),
        MapDbParentSideJoinConditionStoreProvider.getInstance());

    TermType termType = determineTermType(termMap);

    // for IRI term types, make values valid IRIs.
    UnaryOperator<Object> returnValueAdapter = termType == TermType.IRI ? this::iriEncodeResult : v -> v;

    Function<ExpressionEvaluation, Optional<Object>> getValue =
        expressionEvaluation -> functionEvaluation(expressionEvaluation, executionTriplesMapper, returnValueAdapter);

    return Optional.of(getGenerator(termMap, getValue, allowedTermTypes, determineTermType(termMap)));
  }

  private Optional<Object> functionEvaluation(ExpressionEvaluation expressionEvaluation,
      RdfTriplesMapper<?> executionTriplesMapper, UnaryOperator<Object> returnValueAdapter) {
    Flux<Statement> functionExecution = executionTriplesMapper.map(expressionEvaluation);
    return mapExecution(functionExecution, returnValueAdapter);
  }

  private Optional<Object> mapExecution(Flux<Statement> functionExecution, UnaryOperator<Object> returnValueAdapter) {
    return functionExecution.reduceWith(LinkedHashModel::new, (model, statement) -> {
      model.add(statement);
      return model;
    })
        .map(executionStatements -> mapExecution(executionStatements, returnValueAdapter))
        .subscribeOn(Schedulers.boundedElastic())
        .block();
  }

  private Optional<Object> mapExecution(Model executionStatements, UnaryOperator<Object> returnValueAdapter) {
    // TODO: how to handle function map with multiple subject maps?
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

  private TermType determineTermType(TermMap map) {

    TermType termType = map.getTermType();
    if (termType != null) {
      return termType;
    }

    if (map instanceof ObjectMap) {
      ObjectMap objectMap = (ObjectMap) map;
      if (isReferenceTermMap(map) || objectMap.getLanguage() != null || objectMap.getDatatype() != null) {
        return TermType.LITERAL;
      }
    }

    return TermType.IRI;
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

    String iri = baseIri + lexicalForm;
    if (RdfValues.isValidIri(iri)) {
      return valueFactory.createIRI(iri);
    }

    throw new TermGeneratorFactoryException(String.format(
        "Could not generate a valid iri from term lexical form [%s] as-is, or prefixed with base iri [%s]", lexicalForm,
        baseIri));
  }

  private BNode generateBNodeTerm(String lexicalForm) {
    // TODO consider hash of 'lexicalForm' instead
    String id = createValidBNodeId(lexicalForm);
    return valueFactory.createBNode(id);
  }

  private String createValidBNodeId(String lexicalForm) {
    StringBuilder id = new StringBuilder("bnode");
    String suffix = lexicalForm.replaceAll("[^a-zA-Z_0-9-]+", "");
    if (!suffix.isEmpty()) {
      id.append("-")
          .append(suffix);
    }

    return id.toString();
  }

  private String createNaturalRdfLexicalForm(Object value) {
    // TODO https://www.w3.org/TR/r2rml/#dfn-natural-rdf-literal
    return value.toString();
  }

}
