package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.ExpressionField;
import io.carml.model.FunctionExecution;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.Source;
import io.carml.model.Template;
import io.carml.model.impl.CarmlTemplate;
import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import io.carml.model.impl.CarmlTemplate.TextSegment;
import io.carml.util.TypeRef;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.eclipse.rdf4j.model.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class DefaultLogicalViewEvaluatorTest {

    @Mock
    private MatchingLogicalSourceResolverFactory matchingFactory;

    @Mock
    private LogicalView logicalView;

    @Mock
    private LogicalSource logicalSource;

    @Mock
    private Source source;

    @Mock
    @SuppressWarnings("rawtypes")
    private LogicalSourceResolver resolver;

    private DefaultLogicalViewEvaluator evaluator;

    private Function<Source, ResolvedSource<?>> sourceResolver;

    private ResolvedSource<Object> resolvedSource;

    @BeforeEach
    void setUp() {
        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory));
        resolvedSource = ResolvedSource.of("test-source", new TypeRef<>() {});
        sourceResolver = s -> resolvedSource;
    }

    @SuppressWarnings("unchecked")
    private void setupMocks(Flux<LogicalSourceRecord<Object>> recordFlux, ExpressionEvaluation exprEval) {
        when(logicalView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);

        var matchScore = MatchScore.builder().strongMatch().build();
        var resolverFactory = mock(LogicalSourceResolver.LogicalSourceResolverFactory.class);
        var matchedFactory = MatchedLogicalSourceResolverFactory.of(matchScore, resolverFactory);

        when(matchingFactory.apply(logicalSource)).thenReturn(Optional.of(matchedFactory));
        when(resolverFactory.apply(source)).thenReturn(resolver);
        when(resolver.getLogicalSourceRecords(anySet())).thenReturn(rs -> recordFlux);
        when(resolver.getExpressionEvaluationFactory()).thenReturn(rec -> exprEval);
    }

    private ExpressionField mockExpressionField(String fieldName) {
        var field = mock(ExpressionField.class);
        when(field.getFieldName()).thenReturn(fieldName);
        return field;
    }

    private LogicalSourceRecord<Object> createRecord(Object sourceRecord) {
        return LogicalSourceRecord.of(logicalSource, sourceRecord);
    }

    @Test
    void givenSingleReferenceField_whenEvaluated_thenViewIterationHasFieldValue() {
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(nameField));

        ExpressionEvaluation exprEval = expression -> {
            if ("name".equals(expression)) {
                return Optional.of("alice");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getIndex(), is(0));
                    assertThat(iteration.getValue("name"), is(Optional.of("alice")));
                })
                .verifyComplete();
    }

    @Test
    void givenMultipleReferenceFields_whenEvaluated_thenViewIterationHasAllFields() {
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("$.name");

        var ageField = mockExpressionField("age");
        when(ageField.getReference()).thenReturn("$.age");

        // Use a list-backed set to preserve insertion order for consistent field ordering
        when(logicalView.getFields()).thenReturn(Set.of(nameField, ageField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "$.name" -> Optional.of("alice");
            case "$.age" -> Optional.of(30);
            default -> Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getIndex(), is(0));
                    assertThat(iteration.getValue("name"), is(Optional.of("alice")));
                    assertThat(iteration.getValue("age"), is(Optional.of(30)));
                })
                .verifyComplete();
    }

    @Test
    void givenMultiValuedField_whenEvaluated_thenCartesianProductProducesMultipleIterations() {
        var colorField = mockExpressionField("color");
        when(colorField.getReference()).thenReturn("$.colors");
        when(logicalView.getFields()).thenReturn(Set.of(colorField));

        ExpressionEvaluation exprEval = expression -> {
            if ("$.colors".equals(expression)) {
                return Optional.of(List.of("red", "blue"));
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("color"), is(Optional.of("red")));
        assertThat(iterations.get(1).getIndex(), is(1));
        assertThat(iterations.get(1).getValue("color"), is(Optional.of("blue")));
    }

    @Test
    void givenTwoMultiValuedFields_whenEvaluated_thenCartesianProductExpands() {
        var fieldA = mockExpressionField("a");
        when(fieldA.getReference()).thenReturn("$.a");

        var fieldB = mockExpressionField("b");
        when(fieldB.getReference()).thenReturn("$.b");

        when(logicalView.getFields()).thenReturn(Set.of(fieldA, fieldB));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "$.a" -> Optional.of(List.of("x", "y"));
            case "$.b" -> Optional.of(List.of("1", "2"));
            default -> Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // Cartesian product of [x,y] x [1,2] = 4 iterations.
        // The exact order depends on field iteration order in the Set, but size is deterministic.
        assertThat(iterations, hasSize(4));

        // Verify all expected combinations are present
        var valuePairs = iterations.stream()
                .map(it ->
                        it.getValue("a").orElseThrow() + "," + it.getValue("b").orElseThrow())
                .toList();

        assertThat(valuePairs.contains("x,1"), is(true));
        assertThat(valuePairs.contains("x,2"), is(true));
        assertThat(valuePairs.contains("y,1"), is(true));
        assertThat(valuePairs.contains("y,2"), is(true));
    }

    @Test
    void givenTemplateField_whenEvaluated_thenTemplateExpanded() {
        var greetField = mockExpressionField("greeting");
        Template template = CarmlTemplate.of(
                List.of(new TextSegment("Hello "), new ExpressionSegment(0, "name"), new TextSegment("!")));
        when(greetField.getReference()).thenReturn(null);
        when(greetField.getTemplate()).thenReturn(template);
        when(logicalView.getFields()).thenReturn(Set.of(greetField));

        ExpressionEvaluation exprEval = expression -> {
            if ("name".equals(expression)) {
                return Optional.of("alice");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getIndex(), is(0));
                    assertThat(iteration.getValue("greeting"), is(Optional.of("Hello alice!")));
                })
                .verifyComplete();
    }

    @Test
    void givenConstantField_whenEvaluated_thenConstantValueUsed() {
        var constField = mockExpressionField("status");
        when(constField.getReference()).thenReturn(null);
        when(constField.getTemplate()).thenReturn(null);

        var value = mock(Value.class);
        when(value.stringValue()).thenReturn("fixed");
        when(constField.getConstant()).thenReturn(value);

        when(logicalView.getFields()).thenReturn(Set.of(constField));

        ExpressionEvaluation exprEval = expression -> Optional.empty();

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getIndex(), is(0));
                    assertThat(iteration.getValue("status"), is(Optional.of("fixed")));
                })
                .verifyComplete();
    }

    @Test
    void givenEvaluationContextWithLimit_whenEvaluated_thenFluxTruncated() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        var records = Flux.range(0, 5).map(i -> createRecord("record-" + i));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("value");
            }
            return Optional.empty();
        };

        setupMocks(records, exprEval);

        var limitContext = new EvaluationContext() {
            @Override
            public Set<String> getProjectedFields() {
                return Set.of();
            }

            @Override
            public DedupStrategy getDedupStrategy() {
                return DedupStrategy.exact();
            }

            @Override
            public Optional<java.time.Duration> getJoinWindowDuration() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getJoinWindowCount() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getLimit() {
                return Optional.of(3L);
            }
        };

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, limitContext)
                .collectList()
                .block();

        assertThat(iterations, hasSize(3));
    }

    @Test
    void givenViewOnNotLogicalSource_whenEvaluated_thenError() {
        var nestedView = mock(LogicalView.class);
        when(logicalView.getViewOn()).thenReturn(nestedView);

        var defaults = EvaluationContext.defaults();
        var exception = assertThrows(
                LogicalSourceResolverException.class, () -> evaluator.evaluate(logicalView, sourceResolver, defaults));

        assertThat(exception.getMessage(), containsString("LogicalView viewOn must be a LogicalSource"));
    }

    @Test
    void givenNoMatchingResolver_whenEvaluated_thenError() {
        when(logicalView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);
        when(matchingFactory.apply(logicalSource)).thenReturn(Optional.empty());
        when(matchingFactory.getResolverName()).thenReturn("TestResolver");

        var defaults = EvaluationContext.defaults();
        var exception = assertThrows(
                LogicalSourceResolverException.class, () -> evaluator.evaluate(logicalView, sourceResolver, defaults));

        assertThat(exception.getMessage(), containsString("No logical source resolver found"));
    }

    @Test
    void givenFieldWithEmptyReference_whenEvaluated_thenEmptyIterationSkipped() {
        var emptyField = mockExpressionField("missing");
        when(emptyField.getReference()).thenReturn("$.missing");
        when(logicalView.getFields()).thenReturn(Set.of(emptyField));

        // Expression evaluation returns empty for the reference, which produces an empty value list.
        // CartesianProduct with an empty list yields no combinations, so no ViewIterations.
        ExpressionEvaluation exprEval = expression -> Optional.empty();

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .verifyComplete();
    }

    @Test
    void givenMultipleFields_whenEvaluated_thenGetKeysReturnsAllFieldNames() {
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        var ageField = mockExpressionField("age");
        when(ageField.getReference()).thenReturn("age");
        when(logicalView.getFields()).thenReturn(Set.of(nameField, ageField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "name" -> Optional.of("alice");
            case "age" -> Optional.of(30);
            default -> Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getKeys(), containsInAnyOrder("name", "age"));
                    assertThat(iteration.getValue("nonexistent"), is(Optional.empty()));
                })
                .verifyComplete();
    }

    @Test
    void givenTemplateWithMultiValuedExpression_whenEvaluated_thenTemplateExpandsPerValue() {
        var labelField = mockExpressionField("label");
        Template template = CarmlTemplate.of(List.of(new ExpressionSegment(0, "color"), new TextSegment("-item")));
        when(labelField.getReference()).thenReturn(null);
        when(labelField.getTemplate()).thenReturn(template);
        when(logicalView.getFields()).thenReturn(Set.of(labelField));

        ExpressionEvaluation exprEval = expression -> {
            if ("color".equals(expression)) {
                return Optional.of(List.of("red", "blue"));
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getValue("label"), is(Optional.of("red-item")));
        assertThat(iterations.get(1).getValue("label"), is(Optional.of("blue-item")));
    }

    @Test
    void givenTemplateWithEmptyExpression_whenEvaluated_thenNoIterationsEmitted() {
        var field = mockExpressionField("greeting");
        Template template = CarmlTemplate.of(List.of(new TextSegment("Hello "), new ExpressionSegment(0, "name")));
        when(field.getReference()).thenReturn(null);
        when(field.getTemplate()).thenReturn(template);
        when(logicalView.getFields()).thenReturn(Set.of(field));

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), expression -> Optional.empty());

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .verifyComplete();
    }

    @Test
    void givenTemplateWithOnlyTextSegments_whenEvaluated_thenReturnsConstantString() {
        var labelField = mockExpressionField("label");
        Template template = CarmlTemplate.of(List.of(new TextSegment("constant-value")));
        when(labelField.getReference()).thenReturn(null);
        when(labelField.getTemplate()).thenReturn(template);
        when(logicalView.getFields()).thenReturn(Set.of(labelField));

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), expression -> Optional.empty());

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> assertThat(iteration.getValue("label"), is(Optional.of("constant-value"))))
                .verifyComplete();
    }

    @Test
    void givenFieldWithFunctionExecution_whenEvaluated_thenThrowsUnsupportedOperation() {
        var funcField = mock(ExpressionField.class);
        when(funcField.getReference()).thenReturn(null);
        when(funcField.getTemplate()).thenReturn(null);
        when(funcField.getConstant()).thenReturn(null);
        when(funcField.getFunctionExecution()).thenReturn(mock(FunctionExecution.class));
        when(logicalView.getFields()).thenReturn(Set.of(funcField));

        var rec = createRecord("record-1");
        ExpressionEvaluation exprEval = expression -> Optional.empty();
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .expectError(UnsupportedOperationException.class)
                .verify();
    }

    @Test
    void givenMultipleSourceRecords_whenEvaluated_thenAllRecordsProcessed() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        var record1 = createRecord("record-1");
        var record2 = createRecord("record-2");
        var record3 = createRecord("record-3");
        var recordFlux = Flux.just(record1, record2, record3);

        // Use a map to track which source record maps to which return value. Since the
        // ExpressionEvaluationFactory receives the source record, we key off identity.
        var valuesByRecord = java.util.Map.of(
                "record-1", "alpha",
                "record-2", "beta",
                "record-3", "gamma");

        setupMocksWithPerRecordEval(recordFlux, sourceRecord -> expression -> {
            if ("id".equals(expression)) {
                return Optional.ofNullable(valuesByRecord.get((String) sourceRecord));
            }
            return Optional.empty();
        });

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(3));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("alpha")));
        assertThat(iterations.get(1).getIndex(), is(1));
        assertThat(iterations.get(1).getValue("id"), is(Optional.of("beta")));
        assertThat(iterations.get(2).getIndex(), is(2));
        assertThat(iterations.get(2).getValue("id"), is(Optional.of("gamma")));
    }

    /**
     * Variant of setupMocks that accepts a per-record expression evaluation factory, allowing
     * different expression results per source record.
     */
    @SuppressWarnings("unchecked")
    private void setupMocksWithPerRecordEval(
            Flux<LogicalSourceRecord<Object>> recordFlux, Function<Object, ExpressionEvaluation> perRecordEvalFactory) {

        when(logicalView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);

        var matchScore = MatchScore.builder().strongMatch().build();
        var resolverFactory = mock(LogicalSourceResolver.LogicalSourceResolverFactory.class);
        var matchedFactory = MatchedLogicalSourceResolverFactory.of(matchScore, resolverFactory);

        when(matchingFactory.apply(logicalSource)).thenReturn(Optional.of(matchedFactory));
        when(resolverFactory.apply(source)).thenReturn(resolver);
        when(resolver.getLogicalSourceRecords(anySet())).thenReturn(rs -> recordFlux);
        when(resolver.getExpressionEvaluationFactory()).thenReturn(perRecordEvalFactory::apply);
    }
}
