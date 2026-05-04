package io.carml.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ExpressionMap;
import io.carml.model.FunctionExecution;
import io.carml.model.impl.CarmlFunctionExecution;
import io.carml.model.impl.CarmlFunctionMap;
import io.carml.model.impl.CarmlInput;
import io.carml.model.impl.CarmlObjectMap;
import io.carml.model.impl.CarmlParameterMap;
import io.carml.vocab.Rdf.IdlabFn;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FunctionExecutionSupport} that lock in the full FNML execution pipeline
 * (FunctionMap resolution → descriptor lookup → input bindings → execute) when an
 * {@code rml:inputValueMap} resolves to "no value". The RML-FNML spec mandates that NULL be bound
 * to the function parameter rather than short-circuiting before invocation; otherwise the
 * spec-required {@code idlab-fn:isNull} / {@code idlab-fn:isNotNull} condition functions would
 * never be reachable for absent values.
 */
class FunctionExecutionSupportTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private FunctionRegistry registry;

    @BeforeEach
    void setUp() {
        registry = FunctionRegistry.create();
        registry.registerAll(new BuiltInFunctionProvider());
    }

    @Test
    void executeFunctionExecution_invokesIsNotNullWithNullBound_returnsFalse_givenAbsentReference() {
        // input value map references a column that resolves to Optional.empty -- the resolver
        // contract for "no value" / NULL. The engine must still invoke isNotNull, with NULL
        // bound to the str parameter, and return false.
        ExpressionEvaluation expressionEvaluation = ref -> Optional.empty();

        var fnExecution = isNotNullExecution("missingColumn");

        var result = FunctionExecutionSupport.executeFunctionExecution(
                fnExecution.expressionMap,
                fnExecution.functionExecution,
                registry,
                FunctionExecutionSupportTest::evaluate,
                expressionEvaluation,
                (DatatypeMapper) null,
                UnaryOperator.identity());

        assertThat(result, contains(false));
    }

    @Test
    void executeFunctionExecution_invokesIsNotNullWithValueBound_returnsTrue_givenPresentReference() {
        ExpressionEvaluation expressionEvaluation =
                ref -> "presentColumn".equals(ref) ? Optional.of("hello") : Optional.empty();

        var fnExecution = isNotNullExecution("presentColumn");

        var result = FunctionExecutionSupport.executeFunctionExecution(
                fnExecution.expressionMap,
                fnExecution.functionExecution,
                registry,
                FunctionExecutionSupportTest::evaluate,
                expressionEvaluation,
                (DatatypeMapper) null,
                UnaryOperator.identity());

        assertThat(result, contains(true));
    }

    @Test
    void executeFunctionExecution_propagatesFunctionInvocationException_givenThrowingFunction() {
        // Lock in the propagation policy: when the function impl raises, the engine surfaces a
        // FunctionInvocationException rather than swallowing it as graceful degradation.
        IRI throwingIri = VF.createIRI("http://example.com/throws");
        registry.registerAll(new AnnotatedFunctionProvider(new ThrowingTarget()));

        ExpressionEvaluation expressionEvaluation = ref -> Optional.empty();

        var fnExecution = singleInputExecution(throwingIri, VF.createIRI("http://example.com/anyParam"), "anything");
        var expressionMap = fnExecution.expressionMap;
        var functionExecution = fnExecution.functionExecution;
        UnaryOperator<Object> identity = UnaryOperator.identity();

        assertThrows(
                FunctionInvocationException.class,
                () -> FunctionExecutionSupport.executeFunctionExecution(
                        expressionMap,
                        functionExecution,
                        registry,
                        FunctionExecutionSupportTest::evaluate,
                        expressionEvaluation,
                        (DatatypeMapper) null,
                        identity));
    }

    @SuppressWarnings("unused") // discovered reflectively by AnnotatedFunctionProvider
    static class ThrowingTarget {

        @RmlFunction("http://example.com/throws")
        public String alwaysThrows(@RmlParam("http://example.com/anyParam") String input) {
            throw new RuntimeException("boom");
        }
    }

    private static FixtureExecution isNotNullExecution(String reference) {
        return singleInputExecution(IdlabFn.isNotNull, IdlabFn.str, reference);
    }

    private static FixtureExecution singleInputExecution(IRI functionIri, IRI parameterIri, String reference) {
        var functionMap = CarmlFunctionMap.builder().constant(functionIri).build();
        var parameterMap = CarmlParameterMap.builder().constant(parameterIri).build();
        var inputValueMap = CarmlObjectMap.builder().reference(reference).build();
        var input = CarmlInput.builder()
                .parameterMap(parameterMap)
                .inputValueMap(inputValueMap)
                .build();
        var functionExecution = CarmlFunctionExecution.builder()
                .functionMap(functionMap)
                .inputs(Set.of(input))
                .build();
        // The expression map enclosing the FunctionExecution -- a bare ObjectMap suffices because
        // executeFunctionExecution only reads getReturnMap (absent here).
        var expressionMap =
                CarmlObjectMap.builder().functionExecution(functionExecution).build();
        return new FixtureExecution(expressionMap, functionExecution);
    }

    /**
     * Minimal recursive evaluator: handles the constant-IRI FunctionMap / ParameterMap and the
     * reference-valued inputValueMap forms produced by the helpers above. A full evaluator is
     * unnecessary for these tests since the FixtureExecution helpers only generate those two
     * shapes.
     */
    private static List<Object> evaluate(
            ExpressionMap expressionMap, ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        if (expressionMap.getConstant() != null) {
            return List.of(expressionMap.getConstant());
        }
        if (expressionMap.getReference() != null) {
            return expressionEvaluation
                    .apply(expressionMap.getReference())
                    .map(ExpressionEvaluation::extractValues)
                    .orElse(List.of());
        }
        return List.of();
    }

    private record FixtureExecution(ExpressionMap expressionMap, FunctionExecution functionExecution) {}
}
