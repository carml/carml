package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import io.carml.model.Field;
import io.carml.model.FunctionExecution;
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.ReferenceFormulation;
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

    private IterableField mockIterableField(String fieldName, String iterator, Set<io.carml.model.Field> nestedFields) {
        var field = mock(IterableField.class);
        when(field.getFieldName()).thenReturn(fieldName);
        when(field.getIterator()).thenReturn(iterator);
        when(field.getFields()).thenReturn(nestedFields);
        return field;
    }

    @Test
    void givenSingleIterableWithExpressionChildren_whenEvaluated_thenPrefixedKeysProduced() {
        // Root iterable "items" with iterator "$.items[*]", containing expression field "type"
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        // Sub-records: two items returned by the iterator
        var subRecord1 = new Object();
        var subRecord2 = new Object();

        // Root expression evaluation: iterator returns the sub-records
        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord1, subRecord2));
            }
            return Optional.empty();
        };

        // Per-sub-record evaluation factory
        Function<Object, ExpressionEvaluation> perRecordFactory = rec -> expression -> {
            if ("type".equals(expression)) {
                if (rec == subRecord1) {
                    return Optional.of("sword");
                }
                if (rec == subRecord2) {
                    return Optional.of("shield");
                }
            }
            return Optional.empty();
        };

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            return perRecordFactory.apply(rec);
        });

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getValue("items.type"), is(Optional.of("sword")));
        assertThat(iterations.get(1).getValue("items.type"), is(Optional.of("shield")));
    }

    @Test
    void givenExpressionFieldAndIterableField_whenEvaluated_thenCrossProductProduced() {
        // Root expression field "name" + iterable "items" with expression child "type"
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");

        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(logicalView.getFields()).thenReturn(Set.of(nameField, itemsIterable));

        var subRecord1 = new Object();
        var subRecord2 = new Object();

        ExpressionEvaluation rootExprEval = expression -> switch (expression) {
            case "name" -> Optional.of("alice");
            case "$.items[*]" -> Optional.of(List.of(subRecord1, subRecord2));
            default -> Optional.empty();
        };

        Function<Object, ExpressionEvaluation> perRecordFactory = rec -> expression -> {
            if ("type".equals(expression)) {
                if (rec == subRecord1) {
                    return Optional.of("sword");
                }
                if (rec == subRecord2) {
                    return Optional.of("shield");
                }
            }
            return Optional.empty();
        };

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            return perRecordFactory.apply(rec);
        });

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // "items" sorts before "name", but Cartesian product is [items contribution] x [name contribution]
        // Items contribute 2 maps, name contributes 1 map → 2 iterations
        assertThat(iterations, hasSize(2));

        var valuePairs = iterations.stream()
                .map(it -> it.getValue("name").orElseThrow() + ","
                        + it.getValue("items.type").orElseThrow())
                .toList();

        assertThat(valuePairs, containsInAnyOrder("alice,sword", "alice,shield"));
    }

    @Test
    void givenMultiValuedExpressionWithinIterable_whenEvaluated_thenMultipleIterationsPerSubRecord() {
        // Iterable "items" with expression child "tag" that returns multivalued results
        var tagField = mockExpressionField("tag");
        when(tagField.getReference()).thenReturn("tags");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(tagField));
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord1 = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord1));
            }
            return Optional.empty();
        };

        Function<Object, ExpressionEvaluation> perRecordFactory = rec -> expression -> {
            if ("tags".equals(expression) && rec == subRecord1) {
                return Optional.of(List.of("red", "blue"));
            }
            return Optional.empty();
        };

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            return perRecordFactory.apply(rec);
        });

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getValue("items.tag"), is(Optional.of("red")));
        assertThat(iterations.get(1).getValue("items.tag"), is(Optional.of("blue")));
    }

    @Test
    void givenNestedIterables_whenEvaluated_thenRecursiveUnnesting() {
        // items (iterable) → details (iterable) → value (expression)
        var valueField = mockExpressionField("value");
        when(valueField.getReference()).thenReturn("value");

        var detailsIterable = mockIterableField("details", "$.details[*]", Set.of(valueField));
        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(detailsIterable));
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var itemSubRecord = new Object();
        var detailSubRecord1 = new Object();
        var detailSubRecord2 = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(itemSubRecord));
            }
            return Optional.empty();
        };

        Function<Object, ExpressionEvaluation> perRecordFactory = rec -> expression -> {
            if (rec == itemSubRecord && "$.details[*]".equals(expression)) {
                return Optional.of(List.of(detailSubRecord1, detailSubRecord2));
            }
            if (rec == detailSubRecord1 && "value".equals(expression)) {
                return Optional.of("v1");
            }
            if (rec == detailSubRecord2 && "value".equals(expression)) {
                return Optional.of("v2");
            }
            return Optional.empty();
        };

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            return perRecordFactory.apply(rec);
        });

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getValue("items.details.value"), is(Optional.of("v1")));
        assertThat(iterations.get(1).getValue("items.details.value"), is(Optional.of("v2")));
    }

    @Test
    void givenEmptyIterable_whenEvaluated_thenNoIterations() {
        var itemsIterable = mock(IterableField.class);
        when(itemsIterable.getIterator()).thenReturn("$.items[*]");
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        // Iterator returns empty list
        ExpressionEvaluation exprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of());
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .verifyComplete();
    }

    @Test
    void givenIterableWithNoNestedFields_whenEvaluated_thenNoIterations() {
        var itemsIterable = mock(IterableField.class);
        when(itemsIterable.getIterator()).thenReturn("$.items[*]");
        when(itemsIterable.getFields()).thenReturn(Set.of());
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord1 = new Object();

        ExpressionEvaluation exprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord1));
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .verifyComplete();
    }

    @Test
    void givenIterableWithIteratorReturningEmpty_whenEvaluated_thenNoIterations() {
        // Exercises the .orElse(List.of()) branch when iterator expression returns Optional.empty()
        var itemsIterable = mock(IterableField.class);
        when(itemsIterable.getIterator()).thenReturn("$.items[*]");
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        // Iterator returns Optional.empty() (expression not found), distinct from Optional.of(List.of())
        ExpressionEvaluation exprEval = expression -> Optional.empty();

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .verifyComplete();
    }

    @Test
    void givenIterableWithNullNestedFields_whenEvaluated_thenNoIterations() {
        // Exercises the null guard on field.getFields()
        var itemsIterable = mock(IterableField.class);
        when(itemsIterable.getIterator()).thenReturn("$.items[*]");
        when(itemsIterable.getFields()).thenReturn(null);
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord1 = new Object();

        ExpressionEvaluation exprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord1));
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .verifyComplete();
    }

    @Test
    void givenMultipleSourceRecordsWithIterable_whenEvaluated_thenIndicesContinueAcrossRecords() {
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        // Each source record has one sub-record
        var subRecordA = new Object();
        var subRecordB = new Object();

        var record1 = createRecord("record-1");
        var record2 = createRecord("record-2");

        setupMocksWithPerRecordEval(Flux.just(record1, record2), rec -> {
            if (rec == record1.getSourceRecord()) {
                return expression -> {
                    if ("$.items[*]".equals(expression)) {
                        return Optional.of(List.of(subRecordA));
                    }
                    return Optional.empty();
                };
            }
            if (rec == record2.getSourceRecord()) {
                return expression -> {
                    if ("$.items[*]".equals(expression)) {
                        return Optional.of(List.of(subRecordB));
                    }
                    return Optional.empty();
                };
            }
            if (rec == subRecordA) {
                return expression -> {
                    if ("type".equals(expression)) {
                        return Optional.of("sword");
                    }
                    return Optional.empty();
                };
            }
            if (rec == subRecordB) {
                return expression -> {
                    if ("type".equals(expression)) {
                        return Optional.of("shield");
                    }
                    return Optional.empty();
                };
            }
            return expression -> Optional.empty();
        });

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("items.type"), is(Optional.of("sword")));
        assertThat(iterations.get(1).getIndex(), is(1));
        assertThat(iterations.get(1).getValue("items.type"), is(Optional.of("shield")));
    }

    @Test
    void givenUnsupportedFieldType_whenEvaluated_thenThrowsUnsupportedOperation() {
        // Field that is neither ExpressionField nor IterableField
        var unknownField = mock(Field.class);
        when(logicalView.getFields()).thenReturn(Set.of(unknownField));

        ExpressionEvaluation exprEval = expression -> Optional.empty();

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .expectError(UnsupportedOperationException.class)
                .verify();
    }

    // --- Mixed reference formulation tests ---

    @SuppressWarnings("unchecked")
    private MatchingLogicalSourceResolverFactory setupSecondResolverFactory(
            ReferenceFormulation targetRefForm, Function<Object, ExpressionEvaluation> perRecordEvalFactory) {
        var secondMatchingFactory = mock(MatchingLogicalSourceResolverFactory.class);
        var secondResolver = mock(LogicalSourceResolver.class);
        var secondResolverFactory = mock(LogicalSourceResolver.LogicalSourceResolverFactory.class);
        var matchScore = MatchScore.builder().strongMatch().build();
        var matchedFactory = MatchedLogicalSourceResolverFactory.of(matchScore, secondResolverFactory);

        // Match synthetic logical source with the target reference formulation
        when(secondMatchingFactory.apply(any(LogicalSource.class))).thenAnswer(invocation -> {
            LogicalSource ls = invocation.getArgument(0);
            if (targetRefForm.equals(ls.getReferenceFormulation())) {
                return Optional.of(matchedFactory);
            }
            return Optional.empty();
        });

        when(secondResolverFactory.apply(source)).thenReturn(secondResolver);
        when(secondResolver.getExpressionEvaluationFactory()).thenReturn(perRecordEvalFactory::apply);

        return secondMatchingFactory;
    }

    @Test
    void givenIterableWithDifferentReferenceFormulation_whenEvaluated_thenDifferentFactoryUsed() {
        var jsonRefForm = mock(ReferenceFormulation.class);
        var csvRefForm = mock(ReferenceFormulation.class);

        // Parent logical source uses CSV reference formulation
        when(logicalSource.getReferenceFormulation()).thenReturn(csvRefForm);

        var valueField = mockExpressionField("value");
        when(valueField.getReference()).thenReturn("data");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(valueField));
        when(itemsIterable.getReferenceFormulation()).thenReturn(jsonRefForm);
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord1 = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord1));
            }
            return Optional.empty();
        };

        // Default: matchingFactory returns empty for unrecognized logical sources (e.g. synthetic)
        when(matchingFactory.apply(any(LogicalSource.class))).thenReturn(Optional.empty());

        // Set up the main CSV resolver
        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            // Main factory should NOT be called for sub-records
            throw new AssertionError("Main (CSV) factory should not be used for sub-records with JSON ref formulation");
        });

        // Set up a second (JSON) resolver that handles sub-records
        var secondMatchingFactory = setupSecondResolverFactory(jsonRefForm, rec -> expression -> {
            if ("data".equals(expression) && rec == subRecord1) {
                return Optional.of("json-parsed-value");
            }
            return Optional.empty();
        });

        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory, secondMatchingFactory));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("items.value"), is(Optional.of("json-parsed-value")));
    }

    @Test
    void givenIterableWithNullReferenceFormulation_whenEvaluated_thenParentFactoryUsed() {
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        // getReferenceFormulation() returns null by default on mock — parent factory should be used
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord1 = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord1));
            }
            return Optional.empty();
        };

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            // Parent factory is used for sub-records when ref formulation is null
            return expression -> {
                if ("type".equals(expression) && rec == subRecord1) {
                    return Optional.of("parent-factory-value");
                }
                return Optional.empty();
            };
        });

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("items.type"), is(Optional.of("parent-factory-value")));
    }

    @Test
    void givenNestedIterablesWithMixedFormulations_whenEvaluated_thenCorrectFactoryAtEachLevel() {
        var csvRefForm = mock(ReferenceFormulation.class);
        var jsonRefForm = mock(ReferenceFormulation.class);

        when(logicalSource.getReferenceFormulation()).thenReturn(csvRefForm);

        // Structure: items (json) → details (null ref form, inherits json) → value (expression)
        var valueField = mockExpressionField("value");
        when(valueField.getReference()).thenReturn("value");

        var detailsIterable = mockIterableField("details", "$.details[*]", Set.of(valueField));
        // null ref formulation → inherits from parent (json)

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(detailsIterable));
        when(itemsIterable.getReferenceFormulation()).thenReturn(jsonRefForm);

        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var itemSubRecord = new Object();
        var detailSubRecord = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(itemSubRecord));
            }
            return Optional.empty();
        };

        // Default: matchingFactory returns empty for unrecognized logical sources (e.g. synthetic)
        when(matchingFactory.apply(any(LogicalSource.class))).thenReturn(Optional.empty());

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            throw new AssertionError("Main (CSV) factory should not be used for nested records");
        });

        // JSON factory handles both levels of nesting
        var secondMatchingFactory = setupSecondResolverFactory(jsonRefForm, rec -> expression -> {
            if (rec == itemSubRecord && "$.details[*]".equals(expression)) {
                return Optional.of(List.of(detailSubRecord));
            }
            if (rec == detailSubRecord && "value".equals(expression)) {
                return Optional.of("deep-json-value");
            }
            return Optional.empty();
        });

        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory, secondMatchingFactory));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("items.details.value"), is(Optional.of("deep-json-value")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void givenTwoIterablesWithSameReferenceFormulation_whenEvaluated_thenFactoryResolvedOnce() {
        var csvRefForm = mock(ReferenceFormulation.class);
        var jsonRefForm = mock(ReferenceFormulation.class);

        when(logicalSource.getReferenceFormulation()).thenReturn(csvRefForm);

        // Two sibling iterables that both use JSON reference formulation
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");
        var firstIterable = mockIterableField("alpha", "$.alpha[*]", Set.of(typeField));
        when(firstIterable.getReferenceFormulation()).thenReturn(jsonRefForm);

        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        var secondIterable = mockIterableField("beta", "$.beta[*]", Set.of(nameField));
        when(secondIterable.getReferenceFormulation()).thenReturn(jsonRefForm);

        when(logicalView.getFields()).thenReturn(Set.of(firstIterable, secondIterable));

        var alphaSubRecord = new Object();
        var betaSubRecord = new Object();

        ExpressionEvaluation rootExprEval = expression -> switch (expression) {
            case "$.alpha[*]" -> Optional.of(List.of(alphaSubRecord));
            case "$.beta[*]" -> Optional.of(List.of(betaSubRecord));
            default -> Optional.empty();
        };

        // Default: matchingFactory returns empty for unrecognized logical sources (e.g. synthetic)
        when(matchingFactory.apply(any(LogicalSource.class))).thenReturn(Optional.empty());

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            throw new AssertionError("Main factory should not be used for sub-records");
        });

        // Track the second matching factory to verify it's only called once
        var secondMatchingFactory = mock(MatchingLogicalSourceResolverFactory.class);
        var secondResolver = mock(LogicalSourceResolver.class);
        var secondResolverFactory = mock(LogicalSourceResolver.LogicalSourceResolverFactory.class);
        var matchScore = MatchScore.builder().strongMatch().build();
        var matchedFactory = MatchedLogicalSourceResolverFactory.of(matchScore, secondResolverFactory);

        when(secondMatchingFactory.apply(any(LogicalSource.class))).thenAnswer(invocation -> {
            LogicalSource ls = invocation.getArgument(0);
            if (jsonRefForm.equals(ls.getReferenceFormulation())) {
                return Optional.of(matchedFactory);
            }
            return Optional.empty();
        });

        when(secondResolverFactory.apply(source)).thenReturn(secondResolver);
        Function<Object, ExpressionEvaluation> jsonEvalFactory = rec -> expression -> {
            if (rec == alphaSubRecord && "type".equals(expression)) {
                return Optional.of("alpha-type");
            }
            if (rec == betaSubRecord && "name".equals(expression)) {
                return Optional.of("beta-name");
            }
            return Optional.empty();
        };
        when(secondResolver.getExpressionEvaluationFactory()).thenReturn(jsonEvalFactory::apply);

        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory, secondMatchingFactory));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("alpha.type"), is(Optional.of("alpha-type")));
        assertThat(iterations.get(0).getValue("beta.name"), is(Optional.of("beta-name")));

        // Verify the second resolver factory was only applied to source once (caching works)
        verify(secondResolverFactory, times(1)).apply(source);
        verify(secondResolver, times(1)).getExpressionEvaluationFactory();
    }

    @Test
    @SuppressWarnings("unchecked")
    void givenIterableWithUnresolvableReferenceFormulation_whenEvaluated_thenError() {
        var parentRefForm = mock(ReferenceFormulation.class);
        var unknownRefForm = mock(ReferenceFormulation.class);

        when(logicalView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);
        when(logicalSource.getReferenceFormulation()).thenReturn(parentRefForm);

        // Iterable with an unknown reference formulation — no nested field details needed
        // since the error occurs before nested fields are evaluated
        var itemsIterable = mock(IterableField.class);
        when(itemsIterable.getIterator()).thenReturn("$.items[*]");
        when(itemsIterable.getFields()).thenReturn(Set.of(mock(Field.class)));
        when(itemsIterable.getReferenceFormulation()).thenReturn(unknownRefForm);
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord1 = new Object();

        // matchingFactory: matches parent ref form but NOT unknownRefForm
        when(matchingFactory.apply(any(LogicalSource.class))).thenAnswer(invocation -> {
            LogicalSource ls = invocation.getArgument(0);
            if (parentRefForm.equals(ls.getReferenceFormulation())) {
                var matchScore = MatchScore.builder().strongMatch().build();
                var resolverFactory = mock(LogicalSourceResolver.LogicalSourceResolverFactory.class);
                var matched = MatchedLogicalSourceResolverFactory.of(matchScore, resolverFactory);
                when(resolverFactory.apply(source)).thenReturn(resolver);
                when(resolver.getLogicalSourceRecords(anySet())).thenReturn(rs -> Flux.just(createRecord("r1")));
                Function<Object, ExpressionEvaluation> parentEvalFactory = rec -> expression -> {
                    if ("$.items[*]".equals(expression)) {
                        return Optional.of(List.of(subRecord1));
                    }
                    return Optional.empty();
                };
                when(resolver.getExpressionEvaluationFactory()).thenReturn(parentEvalFactory::apply);
                return Optional.of(matched);
            }
            return Optional.empty();
        });

        when(matchingFactory.getResolverName()).thenReturn("ParentResolver");

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .expectErrorMatches(e -> e instanceof LogicalSourceResolverException
                        && e.getMessage().contains("No logical source resolver found"))
                .verify();
    }

    @Test
    void givenIterableWithSameReferenceFormulationAsParent_whenEvaluated_thenNoAdditionalResolverLookup() {
        var parentRefForm = mock(ReferenceFormulation.class);
        when(logicalSource.getReferenceFormulation()).thenReturn(parentRefForm);

        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var iterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(iterable.getReferenceFormulation()).thenReturn(parentRefForm);

        when(logicalView.getFields()).thenReturn(Set.of(iterable));

        var subRecord1 = new Object();

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return expression ->
                        "$.items[*]".equals(expression) ? Optional.of(List.of(subRecord1)) : Optional.empty();
            }
            return expression -> "type".equals(expression) ? Optional.of("cached-value") : Optional.empty();
        });

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("items.type"), is(Optional.of("cached-value")));

        // The parent factory was used (pre-seeded cache) — no additional resolver lookups
        verify(matchingFactory, times(1)).apply(any(LogicalSource.class));
    }

    @Test
    void givenNestedIterablesWithDistinctRefFormulations_whenEvaluated_thenEachLevelUsesCorrectFactory() {
        var csvRefForm = mock(ReferenceFormulation.class);
        var jsonRefForm = mock(ReferenceFormulation.class);
        var xmlRefForm = mock(ReferenceFormulation.class);

        when(logicalSource.getReferenceFormulation()).thenReturn(csvRefForm);

        // Structure: items (json) → details (xml) → value (expression)
        var valueField = mockExpressionField("value");
        when(valueField.getReference()).thenReturn("value");

        var detailsIterable = mockIterableField("details", "$.details[*]", Set.of(valueField));
        when(detailsIterable.getReferenceFormulation()).thenReturn(xmlRefForm);

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(detailsIterable));
        when(itemsIterable.getReferenceFormulation()).thenReturn(jsonRefForm);

        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var itemSubRecord = new Object();
        var detailSubRecord = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(itemSubRecord));
            }
            return Optional.empty();
        };

        // Default: matchingFactory returns empty for synthetic logical sources
        when(matchingFactory.apply(any(LogicalSource.class))).thenReturn(Optional.empty());

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            throw new AssertionError("Main (CSV) factory should not be used for nested records");
        });

        // JSON factory handles items level
        var jsonMatchingFactory = setupSecondResolverFactory(jsonRefForm, rec -> expression -> {
            if (rec == itemSubRecord && "$.details[*]".equals(expression)) {
                return Optional.of(List.of(detailSubRecord));
            }
            return Optional.empty();
        });

        // XML factory handles details level
        var xmlMatchingFactory = setupSecondResolverFactory(xmlRefForm, rec -> expression -> {
            if (rec == detailSubRecord && "value".equals(expression)) {
                return Optional.of("xml-parsed-value");
            }
            return Optional.empty();
        });

        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory, jsonMatchingFactory, xmlMatchingFactory));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("items.details.value"), is(Optional.of("xml-parsed-value")));
    }
}
