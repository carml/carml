package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.functions.FunctionDescriptor;
import io.carml.functions.FunctionRegistry;
import io.carml.functions.ParameterDescriptor;
import io.carml.functions.ReturnDescriptor;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ExpressionMap;
import io.carml.model.FunctionExecution;
import io.carml.model.FunctionMap;
import io.carml.model.Input;
import io.carml.model.ObjectMap;
import io.carml.model.ParameterMap;
import io.carml.model.Template.Segment;
import io.carml.model.impl.CarmlObjectMap;
import io.carml.model.impl.CarmlTemplate;
import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import io.carml.model.impl.CarmlTemplate.TextSegment;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultExpressionMapEvaluatorTest {

    private static final IRI TO_UPPER = SimpleValueFactory.getInstance().createIRI("http://example.org/fn/toUpperCase");

    private static final IRI INPUT_PARAM = SimpleValueFactory.getInstance().createIRI("http://example.org/fn/input");

    private FunctionRegistry functionRegistry;

    private DefaultExpressionMapEvaluator evaluator;

    @BeforeEach
    void setUp() {
        functionRegistry = FunctionRegistry.create();
        functionRegistry.register(new FunctionDescriptor() {
            @Override
            public IRI getFunctionIri() {
                return TO_UPPER;
            }

            @Override
            public List<ParameterDescriptor> getParameters() {
                return List.of(new ParameterDescriptor(INPUT_PARAM, String.class, true));
            }

            @Override
            public List<ReturnDescriptor> getReturns() {
                return List.of(new ReturnDescriptor(null, String.class));
            }

            @Override
            public Object execute(Map<IRI, Object> parameterValues) {
                var value = parameterValues.get(INPUT_PARAM);
                return value == null ? null : value.toString().toUpperCase();
            }
        });

        evaluator = new DefaultExpressionMapEvaluator(functionRegistry);
    }

    @Test
    void evaluate_referenceExpressionMap_resolvesReferenceValue() {
        var expressionMap = CarmlObjectMap.builder().reference("firstName").build();
        ExpressionEvaluation exprEval =
                expression -> "firstName".equals(expression) ? Optional.of("ada") : Optional.empty();
        DatatypeMapper datatypeMapper = field -> Optional.empty();

        var result = evaluator.evaluate(expressionMap, exprEval, datatypeMapper);

        assertThat(result, contains("ada"));
    }

    @Test
    void evaluate_templateExpressionMap_returnsRenderedTemplate() {
        var template = CarmlTemplate.of(
                List.<Segment>of(new TextSegment("http://example.org/"), new ExpressionSegment(0, "id")));
        var expressionMap = CarmlObjectMap.builder().template(template).build();
        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("42") : Optional.empty();
        DatatypeMapper datatypeMapper = field -> Optional.empty();

        var result = evaluator.evaluate(expressionMap, exprEval, datatypeMapper);

        assertThat(result, contains("http://example.org/42"));
    }

    @Test
    void evaluate_missingReference_returnsEmptyList() {
        var expressionMap = CarmlObjectMap.builder().reference("missing").build();
        ExpressionEvaluation exprEval = expression -> Optional.empty();
        DatatypeMapper datatypeMapper = field -> Optional.empty();

        var result = evaluator.evaluate(expressionMap, exprEval, datatypeMapper);

        assertThat(result, is(empty()));
    }

    @Test
    void evaluate_functionExecutionExpressionMap_invokesRegisteredFunction() {
        var expressionMap = withToUpperFunctionExecution("name");
        ExpressionEvaluation exprEval = expression -> "name".equals(expression) ? Optional.of("ada") : Optional.empty();
        DatatypeMapper datatypeMapper = field -> Optional.empty();

        var result = evaluator.evaluate(expressionMap, exprEval, datatypeMapper);

        assertThat(result, contains("ADA"));
    }

    @Test
    void evaluate_constantOnlyExpressionMap_returnsStringValueNotRdfLiteral() {
        var expressionMap = CarmlObjectMap.builder()
                .constant(SimpleValueFactory.getInstance().createLiteral("human"))
                .build();
        ExpressionEvaluation exprEval = expression -> Optional.empty();
        DatatypeMapper datatypeMapper = field -> Optional.empty();

        var result = evaluator.evaluate(expressionMap, exprEval, datatypeMapper);

        assertThat(result, contains("human"));
        assertThat(result.get(0), is(instanceOf(String.class)));
    }

    /**
     * Builds a mocked {@link ExpressionMap} carrying an {@link FunctionExecution} that invokes the
     * registered {@code toUpperCase} function with a reference to {@code inputReference} bound to
     * the {@link #INPUT_PARAM} parameter.
     */
    private static ExpressionMap withToUpperFunctionExecution(String inputReference) {
        var functionMap = mock(FunctionMap.class);
        lenient().when(functionMap.getConstant()).thenReturn(TO_UPPER);
        lenient().when(functionMap.getReference()).thenReturn(null);
        lenient().when(functionMap.getTemplate()).thenReturn(null);
        lenient().when(functionMap.getFunctionExecution()).thenReturn(null);
        lenient().when(functionMap.getFunctionValue()).thenReturn(null);
        lenient().when(functionMap.getConditions()).thenReturn(Set.of());
        lenient().when(functionMap.getReturnMap()).thenReturn(null);

        var parameterMap = mock(ParameterMap.class);
        lenient().when(parameterMap.getConstant()).thenReturn(INPUT_PARAM);
        lenient().when(parameterMap.getReference()).thenReturn(null);
        lenient().when(parameterMap.getTemplate()).thenReturn(null);
        lenient().when(parameterMap.getFunctionExecution()).thenReturn(null);
        lenient().when(parameterMap.getFunctionValue()).thenReturn(null);
        lenient().when(parameterMap.getConditions()).thenReturn(Set.of());
        lenient().when(parameterMap.getReturnMap()).thenReturn(null);

        var inputValueMap = mock(ObjectMap.class);
        lenient().when(inputValueMap.getReference()).thenReturn(inputReference);
        lenient().when(inputValueMap.getConstant()).thenReturn(null);
        lenient().when(inputValueMap.getTemplate()).thenReturn(null);
        lenient().when(inputValueMap.getFunctionExecution()).thenReturn(null);
        lenient().when(inputValueMap.getFunctionValue()).thenReturn(null);
        lenient().when(inputValueMap.getConditions()).thenReturn(Set.of());
        lenient().when(inputValueMap.getReturnMap()).thenReturn(null);

        var input = mock(Input.class);
        when(input.getParameterMap()).thenReturn(parameterMap);
        when(input.getInputValueMap()).thenReturn(inputValueMap);

        var functionExecution = mock(FunctionExecution.class);
        when(functionExecution.getFunctionMap()).thenReturn(functionMap);
        when(functionExecution.getInputs()).thenReturn(Set.of(input));

        var expressionMap = mock(ObjectMap.class);
        when(expressionMap.getFunctionExecution()).thenReturn(functionExecution);
        lenient().when(expressionMap.getConstant()).thenReturn(null);
        lenient().when(expressionMap.getReference()).thenReturn(null);
        lenient().when(expressionMap.getTemplate()).thenReturn(null);
        lenient().when(expressionMap.getFunctionValue()).thenReturn(null);
        lenient().when(expressionMap.getConditions()).thenReturn(Set.of());
        lenient().when(expressionMap.getReturnMap()).thenReturn(null);
        lenient().when(expressionMap.getTermType()).thenReturn(null);
        lenient().when(expressionMap.getLanguageMap()).thenReturn(null);
        lenient().when(expressionMap.getDatatypeMap()).thenReturn(null);
        return expressionMap;
    }
}
