package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.lenient;
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
import io.carml.model.AbstractLogicalSource;
import io.carml.model.ChildMap;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.FunctionExecution;
import io.carml.model.IterableField;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.ParentMap;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.ReferenceFormulation;
import io.carml.model.Source;
import io.carml.model.Template;
import io.carml.model.UniqueAnnotation;
import io.carml.model.impl.CarmlTemplate;
import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import io.carml.model.impl.CarmlTemplate.TextSegment;
import io.carml.util.TypeRef;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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
    private LogicalSourceResolver<Object> resolver;

    private DefaultLogicalViewEvaluator evaluator;

    private Function<Source, ResolvedSource<?>> sourceResolver;

    private ResolvedSource<Object> resolvedSource;

    @BeforeEach
    void setUp() {
        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory));
        resolvedSource = ResolvedSource.of("test-source", new TypeRef<>() {});
        sourceResolver = s -> resolvedSource;
        parentBindings.clear();
        lenient().when(resolver.getDatatypeMapperFactory()).thenReturn(Optional.empty());
    }

    private void setupMocks(Flux<LogicalSourceRecord<Object>> recordFlux, ExpressionEvaluation exprEval) {
        when(logicalView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);

        var matchScore = MatchScore.builder().strongMatch().build();
        LogicalSourceResolver.LogicalSourceResolverFactory<Object> resolverFactory = mock();
        var matchedFactory = MatchedLogicalSourceResolverFactory.of(matchScore, resolverFactory);

        when(matchingFactory.apply(logicalSource)).thenReturn(Optional.of(matchedFactory));
        when(resolverFactory.apply(source)).thenReturn(resolver);
        when(resolver.getLogicalSourceRecords(anySet(), anyMap())).thenReturn(rs -> recordFlux);
        when(resolver.getExpressionEvaluationFactory()).thenReturn(rec -> exprEval);
    }

    private ExpressionField mockExpressionField(String fieldName) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
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
        // Both expansions share the same source record, so both have root # = 0
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("color"), is(Optional.of("red")));
        assertThat(iterations.get(1).getIndex(), is(0));
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
    void givenTwoMultiValuedFieldsWithRefForm_whenEvaluated_thenRefFormMapContainsBothFieldKeys() {
        var refForm = mock(ReferenceFormulation.class);
        when(logicalSource.getReferenceFormulation()).thenReturn(refForm);

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

        assertThat(iterations, hasSize(4));
        iterations.forEach(it -> {
            assertThat(it.getFieldReferenceFormulation("a"), is(Optional.of(refForm)));
            assertThat(it.getFieldReferenceFormulation("b"), is(Optional.of(refForm)));
        });
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

        setupMocksWithPerRecordEval(records, rec -> expression -> {
            if ("id".equals(expression)) {
                return Optional.of("value-" + rec);
            }
            return Optional.empty();
        });

        var limitContext = DefaultEvaluationContext.builder().limit(3L).build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, limitContext)
                .collectList()
                .block();

        assertThat(iterations, hasSize(3));
    }

    @Test
    void givenViewOnNotLogicalSourceOrLogicalView_whenEvaluated_thenError() {
        var unknownSource = mock(AbstractLogicalSource.class);
        when(logicalView.getViewOn()).thenReturn(unknownSource);

        var defaults = EvaluationContext.defaults();
        @SuppressWarnings("ReactiveStreamsUnusedPublisher")
        var exception = assertThrows(
                LogicalSourceResolverException.class, () -> evaluator.evaluate(logicalView, sourceResolver, defaults));

        assertThat(exception.getMessage(), containsString("LogicalView viewOn must be a LogicalSource or LogicalView"));
    }

    @Test
    void givenNoMatchingResolver_whenEvaluated_thenError() {
        when(logicalView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);
        when(matchingFactory.apply(logicalSource)).thenReturn(Optional.empty());
        when(matchingFactory.getResolverName()).thenReturn("TestResolver");

        var defaults = EvaluationContext.defaults();
        @SuppressWarnings("ReactiveStreamsUnusedPublisher")
        var exception = assertThrows(
                LogicalSourceResolverException.class, () -> evaluator.evaluate(logicalView, sourceResolver, defaults));

        assertThat(exception.getMessage(), containsString("No logical source resolver found"));
    }

    @Test
    void givenFieldWithEmptyReference_whenEvaluated_thenIterationEmittedWithNullValue() {
        var emptyField = mockExpressionField("missing");
        when(emptyField.getReference()).thenReturn("$.missing");
        when(logicalView.getFields()).thenReturn(Set.of(emptyField));

        // Expression evaluation returns empty for the reference. The field contributes a
        // single null-valued entry so the Cartesian product still produces an iteration —
        // the engine will simply not generate a term for this field.
        ExpressionEvaluation exprEval = expression -> Optional.empty();

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getValue("missing"), is(Optional.empty()));
                    assertThat(iteration.getKeys(), hasItem("missing"));
                })
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
                    assertThat(iteration.getKeys(), containsInAnyOrder("name", "age", "#", "name.#", "age.#"));
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
    void givenTemplateWithEmptyExpression_whenEvaluated_thenIterationEmittedWithNullValue() {
        var field = mockExpressionField("greeting");
        Template template = CarmlTemplate.of(List.of(new TextSegment("Hello "), new ExpressionSegment(0, "name")));
        when(field.getReference()).thenReturn(null);
        when(field.getTemplate()).thenReturn(template);
        when(logicalView.getFields()).thenReturn(Set.of(field));

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), expression -> Optional.empty());

        // Template with an empty expression segment evaluates to no value. The field contributes
        // a null entry so the iteration is still emitted — the engine will not generate a term.
        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getValue("greeting"), is(Optional.empty()));
                    assertThat(iteration.getKeys(), hasItem("greeting"));
                })
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
    private void setupMocksWithPerRecordEval(
            Flux<LogicalSourceRecord<Object>> recordFlux, Function<Object, ExpressionEvaluation> perRecordEvalFactory) {

        when(logicalView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);

        var matchScore = MatchScore.builder().strongMatch().build();
        LogicalSourceResolver.LogicalSourceResolverFactory<Object> resolverFactory = mock();
        var matchedFactory = MatchedLogicalSourceResolverFactory.of(matchScore, resolverFactory);

        when(matchingFactory.apply(logicalSource)).thenReturn(Optional.of(matchedFactory));
        when(resolverFactory.apply(source)).thenReturn(resolver);
        when(resolver.getLogicalSourceRecords(anySet(), anyMap())).thenReturn(rs -> recordFlux);
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

        // Verify index keys on cross-product iterations
        iterations.forEach(it -> {
            assertThat(it.getValue("#"), is(Optional.of(it.getIndex())));
            assertThat(it.getValue("name.#"), is(Optional.of(0))); // single-valued, always 0
            assertThat(it.getValue("items.type.#"), is(Optional.of(0))); // single-valued per sub-record
        });

        // items.# should be 0 for sword, 1 for shield
        var swordIt = iterations.stream()
                .filter(it -> it.getValue("items.type").equals(Optional.of("sword")))
                .findFirst()
                .orElseThrow();
        var shieldIt = iterations.stream()
                .filter(it -> it.getValue("items.type").equals(Optional.of("shield")))
                .findFirst()
                .orElseThrow();
        assertThat(swordIt.getValue("items.#"), is(Optional.of(0)));
        assertThat(shieldIt.getValue("items.#"), is(Optional.of(1)));
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
        assertThat(iterations.get(0).getValue("items.#"), is(Optional.of(0)));
        assertThat(iterations.get(1).getIndex(), is(1));
        assertThat(iterations.get(1).getValue("items.type"), is(Optional.of("shield")));
        assertThat(iterations.get(1).getValue("items.#"), is(Optional.of(0)));
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

    private MatchingLogicalSourceResolverFactory setupSecondResolverFactory(
            ReferenceFormulation targetRefForm, Function<Object, ExpressionEvaluation> perRecordEvalFactory) {
        var secondMatchingFactory = mock(MatchingLogicalSourceResolverFactory.class);
        LogicalSourceResolver<Object> secondResolver = mock();
        LogicalSourceResolver.LogicalSourceResolverFactory<Object> secondResolverFactory = mock();
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
        lenient().when(secondResolver.getDatatypeMapperFactory()).thenReturn(Optional.empty());

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
        assertThat(iterations.get(0).getFieldReferenceFormulation("items.value"), is(Optional.of(jsonRefForm)));
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
        assertThat(iterations.get(0).getFieldReferenceFormulation("items.details.value"), is(Optional.of(jsonRefForm)));
    }

    @Test
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
        LogicalSourceResolver<Object> secondResolver = mock();
        LogicalSourceResolver.LogicalSourceResolverFactory<Object> secondResolverFactory = mock();
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
        lenient().when(secondResolver.getDatatypeMapperFactory()).thenReturn(Optional.empty());

        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory, secondMatchingFactory));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("alpha.type"), is(Optional.of("alpha-type")));
        assertThat(iterations.get(0).getValue("beta.name"), is(Optional.of("beta-name")));
        assertThat(iterations.get(0).getValue("alpha.#"), is(Optional.of(0)));
        assertThat(iterations.get(0).getValue("beta.#"), is(Optional.of(0)));

        // Verify the second resolver factory was only applied to source once (caching works)
        verify(secondResolverFactory, times(1)).apply(source);
        verify(secondResolver, times(1)).getExpressionEvaluationFactory();
    }

    @Test
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
                LogicalSourceResolver.LogicalSourceResolverFactory<Object> resolverFactory = mock();
                var matched = MatchedLogicalSourceResolverFactory.of(matchScore, resolverFactory);
                when(resolverFactory.apply(source)).thenReturn(resolver);
                when(resolver.getLogicalSourceRecords(anySet(), anyMap()))
                        .thenReturn(rs -> Flux.just(createRecord("r1")));
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
        assertThat(iterations.get(0).getFieldReferenceFormulation("items.details.value"), is(Optional.of(xmlRefForm)));
    }

    // --- View-on-view tests ---

    @Test
    void givenViewOnLogicalSourceWithRefForm_whenEvaluated_thenIterationsTrackFieldRefForms() {
        var refForm = mock(ReferenceFormulation.class);
        when(logicalSource.getReferenceFormulation()).thenReturn(refForm);

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
                    assertThat(iteration.getValue("name"), is(Optional.of("alice")));
                    assertThat(iteration.getFieldReferenceFormulation("name"), is(Optional.of(refForm)));
                    assertThat(iteration.getFieldReferenceFormulation("nonexistent"), is(Optional.empty()));
                })
                .verifyComplete();
    }

    @Test
    void givenViewOnView_whenEvaluated_thenChildFieldsResolvedFromParentIteration() {
        // Parent view: viewOn = logicalSource, field "name" → "alice"
        var parentView = mock(LogicalView.class);
        var parentNameField = mockExpressionField("name");
        when(parentNameField.getReference()).thenReturn("name");
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentNameField));

        // Child view: viewOn = parentView, field "result" referencing parent's "name"
        var childResultField = mockExpressionField("result");
        when(childResultField.getReference()).thenReturn("name");
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childResultField));

        ExpressionEvaluation exprEval = expression -> {
            if ("name".equals(expression)) {
                return Optional.of("alice");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);
        // Re-stub: setupMocks stubs logicalView.getViewOn() → logicalSource, but parentView needs that
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getIndex(), is(0));
                    assertThat(iteration.getValue("result"), is(Optional.of("alice")));
                })
                .verifyComplete();
    }

    @Test
    void givenViewOnViewWithMultipleParentIterations_whenEvaluated_thenChildIterationsSequentiallyIndexed() {
        var parentView = mock(LogicalView.class);
        var parentIdField = mockExpressionField("id");
        when(parentIdField.getReference()).thenReturn("id");
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentIdField));

        var childIdField = mockExpressionField("childId");
        when(childIdField.getReference()).thenReturn("id");
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childIdField));

        var record1 = createRecord("record-1");
        var record2 = createRecord("record-2");

        setupMocksWithPerRecordEval(Flux.just(record1, record2), rec -> expression -> {
            if ("id".equals(expression)) {
                if (rec == record1.getSourceRecord()) {
                    return Optional.of("a");
                }
                if (rec == record2.getSourceRecord()) {
                    return Optional.of("b");
                }
            }
            return Optional.empty();
        });

        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("childId"), is(Optional.of("a")));
        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
        assertThat(iterations.get(0).getValue("childId.#"), is(Optional.of(0)));
        assertThat(iterations.get(1).getIndex(), is(1));
        assertThat(iterations.get(1).getValue("childId"), is(Optional.of("b")));
        assertThat(iterations.get(1).getValue("#"), is(Optional.of(1)));
        assertThat(iterations.get(1).getValue("childId.#"), is(Optional.of(0)));
    }

    @Test
    void givenViewOnViewWithTemplateField_whenEvaluated_thenTemplateExpandedFromParentValues() {
        var parentView = mock(LogicalView.class);

        var firstField = mockExpressionField("first");
        when(firstField.getReference()).thenReturn("first");
        var lastField = mockExpressionField("last");
        when(lastField.getReference()).thenReturn("last");
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(firstField, lastField));

        var fullNameField = mockExpressionField("fullName");
        Template template = CarmlTemplate.of(
                List.of(new ExpressionSegment(0, "first"), new TextSegment(" "), new ExpressionSegment(1, "last")));
        when(fullNameField.getReference()).thenReturn(null);
        when(fullNameField.getTemplate()).thenReturn(template);
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(fullNameField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "first" -> Optional.of("John");
            case "last" -> Optional.of("Doe");
            default -> Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getIndex(), is(0));
                    assertThat(iteration.getValue("fullName"), is(Optional.of("John Doe")));
                })
                .verifyComplete();
    }

    @Test
    void givenViewOnViewWithMissingFieldReference_whenEvaluated_thenThrowsException() {
        var parentView = mock(LogicalView.class);
        var parentNameField = mockExpressionField("name");
        when(parentNameField.getReference()).thenReturn("name");
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentNameField));

        // Child references a field that doesn't exist in the parent iteration
        var childField = mockExpressionField("missing");
        when(childField.getReference()).thenReturn("nonexistent");
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childField));

        ExpressionEvaluation exprEval = expression -> {
            if ("name".equals(expression)) {
                return Optional.of("alice");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .expectErrorSatisfies(error -> {
                    assertThat(
                            error, org.hamcrest.Matchers.instanceOf(ViewIterationExpressionEvaluationException.class));
                    assertThat(error.getMessage(), containsString("Reference to non-existing key 'nonexistent'"));
                })
                .verify();
    }

    @Test
    void givenViewOnViewWithIterableField_whenEvaluated_thenIterableFieldEvaluated() {
        // Parent view produces iteration with "data" field containing a raw value (e.g., JSON string)
        var parentView = mock(LogicalView.class);
        var parentDataField = mockExpressionField("data");
        when(parentDataField.getReference()).thenReturn("data");
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentDataField));

        // Child view: iterable field "items" with iterator "data", declared reference formulation
        var jsonRefForm = mock(ReferenceFormulation.class);

        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var itemsIterable = mockIterableField("items", "data", Set.of(typeField));
        when(itemsIterable.getReferenceFormulation()).thenReturn(jsonRefForm);
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        // Parent expression evaluation: "data" returns a raw JSON blob
        var jsonBlob = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("data".equals(expression)) {
                return Optional.of(jsonBlob);
            }
            return Optional.empty();
        };

        // Default: matchingFactory returns empty for synthetic logical sources
        when(matchingFactory.apply(any(LogicalSource.class))).thenReturn(Optional.empty());

        var rec = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(rec), r -> {
            if (r == rec.getSourceRecord()) {
                return rootExprEval;
            }
            throw new AssertionError("Main factory should not be used for sub-records");
        });

        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        // JSON resolver evaluates nested expressions against the jsonBlob sub-record.
        // ViewIteration holds single values per field (Cartesian product flattens multivalued
        // results), so the iterator "data" produces exactly one sub-record (jsonBlob).
        var secondMatchingFactory = setupSecondResolverFactory(jsonRefForm, r -> expression -> {
            if (r == jsonBlob && "type".equals(expression)) {
                return Optional.of("parsed-type");
            }
            return Optional.empty();
        });

        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory, secondMatchingFactory));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("items.type"), is(Optional.of("parsed-type")));
    }

    @Test
    void givenViewOnViewWithLimit_whenEvaluated_thenChildOutputTruncated() {
        var parentView = mock(LogicalView.class);
        var parentIdField = mockExpressionField("id");
        when(parentIdField.getReference()).thenReturn("id");
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentIdField));

        var childIdField = mockExpressionField("childId");
        when(childIdField.getReference()).thenReturn("id");
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childIdField));

        var records = Flux.range(0, 5).map(i -> createRecord("record-" + i));

        setupMocksWithPerRecordEval(records, rec -> expression -> {
            if ("id".equals(expression)) {
                return Optional.of("val-" + rec);
            }
            return Optional.empty();
        });

        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        var limitContext = DefaultEvaluationContext.builder().limit(2L).build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, limitContext)
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
    }

    @Test
    void givenViewOnViewWithIterableFieldWithoutRefForm_whenEvaluated_thenRefFormInheritedFromParentIteration() {
        // Parent view: logicalSource (CSV) with iterable "items" (JSON ref form)
        // → produces ViewIteration with "items.value" field under JSON ref form.
        // Child view: iterable "items.value" without explicit ref form
        // → should inherit JSON ref form from parent ViewIteration, NOT CSV.
        var csvRefForm = mock(ReferenceFormulation.class);
        var jsonRefForm = mock(ReferenceFormulation.class);

        when(logicalSource.getReferenceFormulation()).thenReturn(csvRefForm);

        // Parent view: iterable field "items" with JSON ref form, containing "value" expression
        var parentValueField = mockExpressionField("value");
        when(parentValueField.getReference()).thenReturn("value");

        var parentItemsIterable = mockIterableField("items", "$.items[*]", Set.of(parentValueField));
        when(parentItemsIterable.getReferenceFormulation()).thenReturn(jsonRefForm);

        var parentView = mock(LogicalView.class);
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentItemsIterable));

        // Child view: iterable "items.value" with NO ref form, containing "detail" expression
        var childDetailField = mockExpressionField("detail");
        when(childDetailField.getReference()).thenReturn("detail");

        var childIterable = mockIterableField("nested", "items.value", Set.of(childDetailField));
        // getReferenceFormulation() returns null by default on mock — should inherit from parent

        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childIterable));

        var itemSubRecord = new Object();
        var nestedSubRecord = new Object();

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

        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        // JSON factory: handles parent's iterable sub-records AND child's iterable sub-records
        // (because child inherits JSON ref form from parent ViewIteration)
        var jsonMatchingFactory = setupSecondResolverFactory(jsonRefForm, rec -> expression -> {
            if (rec == itemSubRecord && "value".equals(expression)) {
                // Parent iterable: produces a raw blob that becomes the ViewIteration value
                return Optional.of(nestedSubRecord);
            }
            if (rec == nestedSubRecord && "detail".equals(expression)) {
                // Child iterable: JSON factory resolves "detail" from the nested sub-record
                return Optional.of("json-inherited-detail");
            }
            return Optional.empty();
        });

        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory, jsonMatchingFactory));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("nested.detail"), is(Optional.of("json-inherited-detail")));
        assertThat(iterations.get(0).getFieldReferenceFormulation("nested.detail"), is(Optional.of(jsonRefForm)));
    }

    @Test
    void givenViewOnViewWithIterableFieldWithExplicitRefForm_whenEvaluated_thenExplicitRefFormOverridesParent() {
        // Parent view: logicalSource (CSV) with iterable "items" (JSON ref form)
        // → produces ViewIteration with "items.value" under JSON ref form.
        // Child view: iterable "items.value" with explicit XML ref form
        // → should use XML ref form, NOT JSON from parent.
        var csvRefForm = mock(ReferenceFormulation.class);
        var jsonRefForm = mock(ReferenceFormulation.class);
        var xmlRefForm = mock(ReferenceFormulation.class);

        when(logicalSource.getReferenceFormulation()).thenReturn(csvRefForm);

        var parentValueField = mockExpressionField("value");
        when(parentValueField.getReference()).thenReturn("value");

        var parentItemsIterable = mockIterableField("items", "$.items[*]", Set.of(parentValueField));
        when(parentItemsIterable.getReferenceFormulation()).thenReturn(jsonRefForm);

        var parentView = mock(LogicalView.class);
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentItemsIterable));

        var childDetailField = mockExpressionField("detail");
        when(childDetailField.getReference()).thenReturn("detail");

        var childIterable = mockIterableField("nested", "items.value", Set.of(childDetailField));
        when(childIterable.getReferenceFormulation()).thenReturn(xmlRefForm);

        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childIterable));

        var itemSubRecord = new Object();
        var nestedSubRecord = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(itemSubRecord));
            }
            return Optional.empty();
        };

        when(matchingFactory.apply(any(LogicalSource.class))).thenReturn(Optional.empty());

        var record1 = createRecord("record-1");
        setupMocksWithPerRecordEval(Flux.just(record1), rec -> {
            if (rec == record1.getSourceRecord()) {
                return rootExprEval;
            }
            throw new AssertionError("Main (CSV) factory should not be used for nested records");
        });

        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        // JSON factory: handles parent's iterable
        var jsonMatchingFactory = setupSecondResolverFactory(jsonRefForm, rec -> expression -> {
            if (rec == itemSubRecord && "value".equals(expression)) {
                return Optional.of(nestedSubRecord);
            }
            return Optional.empty();
        });

        // XML factory: handles child's iterable (explicit override)
        var xmlMatchingFactory = setupSecondResolverFactory(xmlRefForm, rec -> expression -> {
            if (rec == nestedSubRecord && "detail".equals(expression)) {
                return Optional.of("xml-explicit-detail");
            }
            return Optional.empty();
        });

        evaluator = new DefaultLogicalViewEvaluator(Set.of(matchingFactory, jsonMatchingFactory, xmlMatchingFactory));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("nested.detail"), is(Optional.of("xml-explicit-detail")));
        assertThat(iterations.get(0).getFieldReferenceFormulation("nested.detail"), is(Optional.of(xmlRefForm)));
    }

    @Test
    void givenThreeLevelViewChain_whenEvaluated_thenRecursivelyResolved() {
        // grandparent (viewOn = logicalSource) → parent (viewOn = grandparent) → child (viewOn = parent)
        var grandparentView = mock(LogicalView.class);
        var parentView = mock(LogicalView.class);

        var gpNameField = mockExpressionField("name");
        when(gpNameField.getReference()).thenReturn("name");
        when(grandparentView.getViewOn()).thenReturn(logicalSource);
        when(grandparentView.getFields()).thenReturn(Set.of(gpNameField));

        var parentField = mockExpressionField("parentName");
        when(parentField.getReference()).thenReturn("name");
        when(parentView.getViewOn()).thenReturn(grandparentView);
        when(parentView.getFields()).thenReturn(Set.of(parentField));

        var childField = mockExpressionField("childName");
        when(childField.getReference()).thenReturn("parentName");
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childField));

        ExpressionEvaluation exprEval = expression -> {
            if ("name".equals(expression)) {
                return Optional.of("alice");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);
        // Re-stub: setupMocks stubs logicalView.getViewOn(), restore view chain
        when(grandparentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getViewOn()).thenReturn(grandparentView);
        when(logicalView.getViewOn()).thenReturn(parentView);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getIndex(), is(0));
                    assertThat(iteration.getValue("childName"), is(Optional.of("alice")));
                })
                .verifyComplete();
    }

    // --- Index key tests ---

    @Test
    void givenSingleField_whenEvaluated_thenRootIndexKeyPresent() {
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
                    assertThat(iteration.getValue("#"), is(Optional.of(0)));
                    assertThat(iteration.getIndex(), is(0));
                })
                .verifyComplete();
    }

    @Test
    void givenMultipleSourceRecords_whenEvaluated_thenRootIndexKeySequential() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        var record1 = createRecord("record-1");
        var record2 = createRecord("record-2");
        var record3 = createRecord("record-3");

        var valuesByRecord = Map.of(
                "record-1", "alpha",
                "record-2", "beta",
                "record-3", "gamma");

        setupMocksWithPerRecordEval(Flux.just(record1, record2, record3), sourceRecord -> expression -> {
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
        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
        assertThat(iterations.get(1).getValue("#"), is(Optional.of(1)));
        assertThat(iterations.get(2).getValue("#"), is(Optional.of(2)));
    }

    @Test
    void givenSingleValuedExpressionField_whenEvaluated_thenFieldIndexKeyIsZero() {
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
                .assertNext(iteration -> assertThat(iteration.getValue("name.#"), is(Optional.of(0))))
                .verifyComplete();
    }

    @Test
    void givenMultiValuedExpressionField_whenEvaluated_thenFieldIndexKeysSequential() {
        var colorField = mockExpressionField("color");
        when(colorField.getReference()).thenReturn("$.colors");
        when(logicalView.getFields()).thenReturn(Set.of(colorField));

        ExpressionEvaluation exprEval = expression -> {
            if ("$.colors".equals(expression)) {
                return Optional.of(List.of("red", "blue", "green"));
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(3));
        assertThat(iterations.get(0).getValue("color"), is(Optional.of("red")));
        assertThat(iterations.get(0).getValue("color.#"), is(Optional.of(0)));
        assertThat(iterations.get(1).getValue("color"), is(Optional.of("blue")));
        assertThat(iterations.get(1).getValue("color.#"), is(Optional.of(1)));
        assertThat(iterations.get(2).getValue("color"), is(Optional.of("green")));
        assertThat(iterations.get(2).getValue("color.#"), is(Optional.of(2)));
    }

    @Test
    void givenIterableField_whenEvaluated_thenIterableIndexKeyPresent() {
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord1 = new Object();
        var subRecord2 = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord1, subRecord2));
            }
            return Optional.empty();
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

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getValue("items.#"), is(Optional.of(0)));
        assertThat(iterations.get(0).getValue("items.type"), is(Optional.of("sword")));
        assertThat(iterations.get(1).getValue("items.#"), is(Optional.of(1)));
        assertThat(iterations.get(1).getValue("items.type"), is(Optional.of("shield")));
    }

    @Test
    void givenNestedIterables_whenEvaluated_thenIndexKeysAtEachLevel() {
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

        // items.# should be 0 for both (single item sub-record)
        assertThat(iterations.get(0).getValue("items.#"), is(Optional.of(0)));
        assertThat(iterations.get(1).getValue("items.#"), is(Optional.of(0)));

        // items.details.# should be 0 and 1
        assertThat(iterations.get(0).getValue("items.details.#"), is(Optional.of(0)));
        assertThat(iterations.get(0).getValue("items.details.value"), is(Optional.of("v1")));
        assertThat(iterations.get(1).getValue("items.details.#"), is(Optional.of(1)));
        assertThat(iterations.get(1).getValue("items.details.value"), is(Optional.of("v2")));
    }

    @Test
    void givenCartesianProductOfMultiValuedFields_whenEvaluated_thenCorrectPerFieldIndexKeys() {
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

        assertThat(iterations, hasSize(4));

        // Fields are sorted alphabetically: "a" then "b"
        // Cartesian product: (a=x,a.#=0) x (b=1,b.#=0), (a=x,a.#=0) x (b=2,b.#=1),
        //                    (a=y,a.#=1) x (b=1,b.#=0), (a=y,a.#=1) x (b=2,b.#=1)
        var combos = iterations.stream()
                .map(it -> it.getValue("a").orElseThrow() + ","
                        + it.getValue("a.#").orElseThrow() + ","
                        + it.getValue("b").orElseThrow() + ","
                        + it.getValue("b.#").orElseThrow())
                .toList();

        assertThat(combos, containsInAnyOrder("x,0,1,0", "x,0,2,1", "y,1,1,0", "y,1,2,1"));

        // Every iteration has a root # key with typed Integer value
        iterations.forEach(it -> assertThat(it.getValue("#"), is(Optional.of(it.getIndex()))));
    }

    // --- Join tests ---

    /**
     * Record holding per-view resolver wiring (logical source → resolver → matched factory).
     */
    private record ViewResolverBinding(
            LogicalSource logicalSource, Source source, LogicalSourceResolver<Object> resolver) {}

    private final List<ViewResolverBinding> parentBindings = new ArrayList<>();

    /**
     * Sets up the child view's resolver on the class-level mocks ({@link #logicalView},
     * {@link #logicalSource}, {@link #source}, {@link #resolver}) and configures the expression
     * evaluation factory.
     */
    private void setupChildView(Flux<LogicalSourceRecord<Object>> recordFlux, ExpressionEvaluation exprEval) {
        when(logicalView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);
        when(resolver.getLogicalSourceRecords(anySet(), anyMap())).thenReturn(rs -> recordFlux);
        when(resolver.getExpressionEvaluationFactory()).thenReturn(rec -> exprEval);
    }

    /**
     * Same as {@link #setupChildView} but with per-record expression evaluation.
     */
    private void setupChildViewPerRecord(
            Flux<LogicalSourceRecord<Object>> recordFlux, Function<Object, ExpressionEvaluation> perRecordEvalFactory) {
        when(logicalView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);
        when(resolver.getLogicalSourceRecords(anySet(), anyMap())).thenReturn(rs -> recordFlux);
        when(resolver.getExpressionEvaluationFactory()).thenReturn(perRecordEvalFactory::apply);
    }

    /**
     * Creates a mock parent LogicalView backed by the given rows. Must call
     * {@link #buildJoinEvaluator()} after all parent views are set up.
     */
    private LogicalView setupParentView(List<Map<String, Object>> parentRows) {
        var parentView = mock(LogicalView.class);
        var parentLogicalSource = mock(LogicalSource.class);
        var parentSource = mock(Source.class);

        when(parentView.getViewOn()).thenReturn(parentLogicalSource);
        when(parentLogicalSource.getSource()).thenReturn(parentSource);

        LogicalSourceResolver<Object> parentResolver = mock();

        var parentRecords = parentRows.stream()
                .map(row -> LogicalSourceRecord.of(parentLogicalSource, (Object) row))
                .toList();

        when(parentResolver.getLogicalSourceRecords(anySet(), anyMap()))
                .thenReturn(rs -> Flux.fromIterable(parentRecords));
        var evalsByRecord = new IdentityHashMap<Object, ExpressionEvaluation>();
        for (var row : parentRows) {
            evalsByRecord.put(row, expression -> Optional.ofNullable(row.get(expression)));
        }
        LogicalSourceResolver.ExpressionEvaluationFactory<Object> parentExprEvalFactory = evalsByRecord::get;
        when(parentResolver.getExpressionEvaluationFactory()).thenReturn(parentExprEvalFactory);
        when(parentResolver.getDatatypeMapperFactory()).thenReturn(Optional.empty());

        if (!parentRows.isEmpty()) {
            var fieldNames = parentRows.get(0).keySet();
            var parentFields = fieldNames.stream()
                    .map(name -> {
                        var f = mockExpressionField(name);
                        when(f.getReference()).thenReturn(name);
                        return (Field) f;
                    })
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            when(parentView.getFields()).thenReturn(parentFields);
        } else {
            when(parentView.getFields()).thenReturn(Set.of());
        }

        parentBindings.add(new ViewResolverBinding(parentLogicalSource, parentSource, parentResolver));

        return parentView;
    }

    /**
     * Builds the evaluator with a combined matching factory that dispatches to the child resolver
     * and all parent resolvers. Call after all {@link #setupParentView} calls.
     */
    private void buildJoinEvaluator() {
        var combinedFactory = mock(MatchingLogicalSourceResolverFactory.class);
        when(combinedFactory.apply(any(LogicalSource.class))).thenAnswer(invocation -> {
            LogicalSource ls = invocation.getArgument(0);
            if (ls == logicalSource) {
                LogicalSourceResolver.LogicalSourceResolverFactory<Object> resolverFactory = mock();
                when(resolverFactory.apply(source)).thenReturn(resolver);
                var matchedFactory = MatchedLogicalSourceResolverFactory.of(
                        MatchScore.builder().strongMatch().build(), resolverFactory);
                return Optional.of(matchedFactory);
            }
            for (var binding : parentBindings) {
                if (ls == binding.logicalSource()) {
                    LogicalSourceResolver.LogicalSourceResolverFactory<Object> resolverFactory = mock();
                    when(resolverFactory.apply(binding.source())).thenReturn(binding.resolver());
                    var matchedFactory = MatchedLogicalSourceResolverFactory.of(
                            MatchScore.builder().strongMatch().build(), resolverFactory);
                    return Optional.of(matchedFactory);
                }
            }
            return Optional.empty();
        });

        evaluator = new DefaultLogicalViewEvaluator(Set.of(combinedFactory));
        sourceResolver = s -> {
            if (s == source) {
                return resolvedSource;
            }
            return ResolvedSource.of("parent-source", new TypeRef<>() {});
        };
    }

    private Join mockJoinCondition(String childRef, String parentRef) {
        var join = mock(Join.class);
        var childMap = mock(ChildMap.class);
        var parentMap = mock(ParentMap.class);
        lenient().when(childMap.getReference()).thenReturn(childRef);
        lenient().when(childMap.getExpressionMapExpressionSet()).thenReturn(Set.of(childRef));
        lenient().when(parentMap.getReference()).thenReturn(parentRef);
        lenient().when(parentMap.getExpressionMapExpressionSet()).thenReturn(Set.of(parentRef));
        lenient().when(join.getChildMap()).thenReturn(childMap);
        lenient().when(join.getParentMap()).thenReturn(parentMap);
        return join;
    }

    private LogicalViewJoin mockLogicalViewJoin(
            LogicalView parentView, Set<Join> conditions, Set<ExpressionField> joinFields) {
        var lvJoin = mock(LogicalViewJoin.class);
        when(lvJoin.getParentLogicalView()).thenReturn(parentView);
        when(lvJoin.getJoinConditions()).thenReturn(conditions);
        when(lvJoin.getFields()).thenReturn(joinFields);
        return lvJoin;
    }

    @Test
    void givenLeftJoinAllMatch_whenEvaluated_thenChildrenExtendedWithJoinFields() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(idField, nameField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "id" -> Optional.of("1");
            case "name" -> Optional.of("alice");
            default -> Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("name"), is(Optional.of("alice")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.of("NYC")));
        assertThat(iterations.get(0).getValue("city.#"), is(Optional.of(0)));
    }

    @Test
    void givenLeftJoinNoMatch_whenEvaluated_thenChildPreservedWithoutJoinFields() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("99");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        lenient().when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("99")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.empty()));
    }

    @Test
    void givenInnerJoinAllMatch_whenEvaluated_thenChildrenExtended() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("1");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getInnerJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.of("NYC")));
    }

    @Test
    void givenInnerJoinNoMatch_whenEvaluated_thenChildDropped() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("99");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        lenient().when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getInnerJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(0));
    }

    @Test
    void givenMultipleParentMatches_whenEvaluated_thenMultipleOutputIterations() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("1");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC"), Map.of("pid", "1", "city", "LA")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        var cities = iterations.stream()
                .map(it -> it.getValue("city").orElseThrow().toString())
                .toList();
        assertThat(cities, containsInAnyOrder("NYC", "LA"));
    }

    @Test
    void givenCompositeJoinCondition_whenEvaluated_thenCompositeKeyMatching() {
        var aField = mockExpressionField("a");
        when(aField.getReference()).thenReturn("a");
        var bField = mockExpressionField("b");
        when(bField.getReference()).thenReturn("b");
        when(logicalView.getFields()).thenReturn(Set.of(aField, bField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "a" -> Optional.of("1");
            case "b" -> Optional.of("x");
            default -> Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(
                List.of(Map.of("pa", "1", "pb", "x", "val", "match"), Map.of("pa", "1", "pb", "y", "val", "no-match")));
        buildJoinEvaluator();

        var valJoinField = mockExpressionField("val");
        when(valJoinField.getReference()).thenReturn("val");

        var cond1 = mockJoinCondition("a", "pa");
        var cond2 = mockJoinCondition("b", "pb");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(cond1, cond2), Set.of(valJoinField));
        when(logicalView.getInnerJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("val"), is(Optional.of("match")));
    }

    @Test
    void givenTwoLeftJoins_whenEvaluated_thenBothAppliedSequentially() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("1");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView1 = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));
        var parentView2 = setupParentView(List.of(Map.of("pid", "1", "color", "blue")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");
        var cond1 = mockJoinCondition("id", "pid");
        var lvJoin1 = mockLogicalViewJoin(parentView1, Set.of(cond1), Set.of(cityJoinField));

        var colorJoinField = mockExpressionField("color");
        when(colorJoinField.getReference()).thenReturn("color");
        var cond2 = mockJoinCondition("id", "pid");
        var lvJoin2 = mockLogicalViewJoin(parentView2, Set.of(cond2), Set.of(colorJoinField));

        var leftJoins = new LinkedHashSet<LogicalViewJoin>();
        leftJoins.add(lvJoin1);
        leftJoins.add(lvJoin2);
        when(logicalView.getLeftJoins()).thenReturn(leftJoins);

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.of("NYC")));
        assertThat(iterations.get(0).getValue("color"), is(Optional.of("blue")));
    }

    @Test
    void givenInnerAndLeftJoinCombo_whenEvaluated_thenAppliedSequentially() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        var record1 = createRecord("record-1");
        var record2 = createRecord("record-2");

        setupChildViewPerRecord(Flux.just(record1, record2), sourceRecord -> expression -> {
            if ("id".equals(expression)) {
                if (sourceRecord == record1.getSourceRecord()) {
                    return Optional.of("1");
                }
                if (sourceRecord == record2.getSourceRecord()) {
                    return Optional.of("2");
                }
            }
            return Optional.empty();
        });

        var innerParentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));
        var leftParentView = setupParentView(List.of(Map.of("pid", "1", "color", "blue")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");
        var innerCond = mockJoinCondition("id", "pid");
        var innerJoin = mockLogicalViewJoin(innerParentView, Set.of(innerCond), Set.of(cityJoinField));
        when(logicalView.getInnerJoins()).thenReturn(Set.of(innerJoin));

        var colorJoinField = mockExpressionField("color");
        when(colorJoinField.getReference()).thenReturn("color");
        var leftCond = mockJoinCondition("id", "pid");
        var leftJoin = mockLogicalViewJoin(leftParentView, Set.of(leftCond), Set.of(colorJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(leftJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.of("NYC")));
        assertThat(iterations.get(0).getValue("color"), is(Optional.of("blue")));
    }

    @Test
    void givenJoinField_whenEvaluated_thenJoinFieldHasIndexKey() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("1");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("city.#"), is(Optional.of(0)));
    }

    @Test
    void givenEmptyParentView_whenLeftJoinEvaluated_thenChildPreserved() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("1");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of());
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        lenient().when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.empty()));
    }

    @Test
    void givenEmptyParentView_whenInnerJoinEvaluated_thenAllChildrenDropped() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("1");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of());
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        lenient().when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getInnerJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(0));
    }

    @Test
    void givenInnerJoinDroppingIterations_whenEvaluated_thenSourceRecordIndexIsPreserved() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        var record1 = createRecord("record-1");
        var record2 = createRecord("record-2");
        var record3 = createRecord("record-3");

        setupChildViewPerRecord(Flux.just(record1, record2, record3), sourceRecord -> expression -> {
            if ("id".equals(expression)) {
                if (sourceRecord == record1.getSourceRecord()) {
                    return Optional.of("1");
                }
                if (sourceRecord == record2.getSourceRecord()) {
                    return Optional.of("2");
                }
                if (sourceRecord == record3.getSourceRecord()) {
                    return Optional.of("3");
                }
            }
            return Optional.empty();
        });

        var parentView = setupParentView(List.of(Map.of("pid", "1", "val", "a"), Map.of("pid", "3", "val", "c")));
        buildJoinEvaluator();

        var valJoinField = mockExpressionField("val");
        when(valJoinField.getReference()).thenReturn("val");
        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(valJoinField));
        when(logicalView.getInnerJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        // Root # reflects source record position, not sequential post-join index.
        // Records 1 and 3 match (source indices 0 and 2); record 2 is dropped by inner join.
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
        assertThat(iterations.get(1).getIndex(), is(2));
        assertThat(iterations.get(1).getValue("#"), is(Optional.of(2)));
    }

    @Test
    void givenLeftJoinWithNonExistingJoinFieldReference_whenEvaluated_thenThrowsException() {
        // Parent view has "pid" field but NOT "city". Join field references non-existing key "city"
        // in the parent view — validation rejects this before any data access.
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("1") : Optional.empty();

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        // Parent has "pid"=1 but NO "city" key
        var parentView = setupParentView(List.of(Map.of("pid", "1")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        lenient().when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .expectErrorSatisfies(error -> {
                    assertThat(
                            error, org.hamcrest.Matchers.instanceOf(ViewIterationExpressionEvaluationException.class));
                    assertThat(error.getMessage(), containsString("Reference to non-existing key 'city'"));
                })
                .verify();
    }

    @Test
    void givenJoinFieldReferencingItKey_whenEvaluated_thenThrowsException() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("1") : Optional.empty();

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1")));
        buildJoinEvaluator();

        var itJoinField = mockExpressionField("itResult");
        when(itJoinField.getReference()).thenReturn("<it>");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(itJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .expectErrorSatisfies(error -> {
                    assertThat(
                            error, org.hamcrest.Matchers.instanceOf(ViewIterationExpressionEvaluationException.class));
                    assertThat(error.getMessage(), containsString("Reference to root iterable record key '<it>'"));
                    assertThat(error.getMessage(), containsString("is not a referenceable key in a logical view"));
                })
                .verify();
    }

    @Test
    void givenJoinFieldReferencingIterableRecordKey_whenEvaluated_thenThrowsException() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("1") : Optional.empty();

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        // Parent view with an iterable field "items" containing nested "type"
        var parentView = mock(LogicalView.class);
        var parentLogicalSource = mock(LogicalSource.class);
        var parentSource = mock(Source.class);

        when(parentView.getViewOn()).thenReturn(parentLogicalSource);
        when(parentLogicalSource.getSource()).thenReturn(parentSource);

        LogicalSourceResolver<Object> parentResolver = mock();

        var subRecord = new Object();
        var parentRecord = LogicalSourceRecord.of(parentLogicalSource, (Object) "parent-rec");

        when(parentResolver.getLogicalSourceRecords(anySet(), anyMap())).thenReturn(rs -> Flux.just(parentRecord));
        LogicalSourceResolver.ExpressionEvaluationFactory<Object> parentExprEvalFactory = rec2 -> expression -> {
            if ("pid".equals(expression)) return Optional.of("1");
            if ("$.items[*]".equals(expression)) return Optional.of(List.of(subRecord));
            if ("type".equals(expression)) return Optional.of("sword");
            return Optional.empty();
        };
        when(parentResolver.getExpressionEvaluationFactory()).thenReturn(parentExprEvalFactory);
        when(parentResolver.getDatatypeMapperFactory()).thenReturn(Optional.empty());

        // Parent view has "pid" expression field + "items" iterable field with nested "type"
        var pidField = mockExpressionField("pid");
        when(pidField.getReference()).thenReturn("pid");
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");
        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(parentView.getFields()).thenReturn(Set.of(pidField, itemsIterable));

        parentBindings.add(new ViewResolverBinding(parentLogicalSource, parentSource, parentResolver));
        buildJoinEvaluator();

        // Join field references "items" — the iterable record key, not a nested field
        var itemsJoinField = mockExpressionField("itemsResult");
        when(itemsJoinField.getReference()).thenReturn("items");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(itemsJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .expectErrorSatisfies(error -> {
                    assertThat(
                            error, org.hamcrest.Matchers.instanceOf(ViewIterationExpressionEvaluationException.class));
                    assertThat(error.getMessage(), containsString("Reference to iterable record key 'items'"));
                    assertThat(error.getMessage(), containsString("Use its nested field names instead"));
                })
                .verify();
    }

    @Test
    void givenJoinFieldWithMultipleValues_whenEvaluated_thenMultipleRowsPerParentMatch() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("1") : Optional.empty();

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        // Parent has one matching record with multivalued "tags"
        var parentView = setupParentView(List.of(Map.of("pid", "1", "tags", List.of("a", "b"))));
        buildJoinEvaluator();

        var tagsJoinField = mockExpressionField("tags");
        when(tagsJoinField.getReference()).thenReturn("tags");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(tagsJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // Multivalued parent source field → parent view produces 2 iterations (one per value).
        // Both parent iterations match child on pid=1, so child is extended twice.
        assertThat(iterations, hasSize(2));
        var tagValues = iterations.stream()
                .map(it -> it.getValue("tags").orElseThrow().toString())
                .toList();
        assertThat(tagValues, containsInAnyOrder("a", "b"));

        // Running index counts across parent matches: first match → tags.#=0, second → tags.#=1
        assertThat(iterations.get(0).getValue("tags.#"), is(Optional.of(0)));
        assertThat(iterations.get(1).getValue("tags.#"), is(Optional.of(1)));
    }

    // --- Deduplication tests ---

    @Test
    void givenExactDedupWithDuplicateSourceRecords_whenEvaluated_thenDuplicatesRemoved() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(idField, nameField));

        var rec1 = createRecord("record-1");
        var rec2 = createRecord("record-2");

        // Both records evaluate to the same field values
        setupMocksWithPerRecordEval(Flux.just(rec1, rec2), rec -> expression -> switch (expression) {
            case "id" -> Optional.of("1");
            case "name" -> Optional.of("alice");
            default -> Optional.empty();
        });

        var exactDedupContext = DefaultEvaluationContext.builder()
                .dedupStrategy(DedupStrategy.exact())
                .build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, exactDedupContext)
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("name"), is(Optional.of("alice")));
    }

    @Test
    void givenExactDedupWithDistinctRecords_whenEvaluated_thenAllPreserved() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(idField, nameField));

        var rec1 = createRecord("record-1");
        var rec2 = createRecord("record-2");

        setupMocksWithPerRecordEval(Flux.just(rec1, rec2), rec -> expression -> {
            if (rec == rec1.getSourceRecord()) {
                return switch (expression) {
                    case "id" -> Optional.of("1");
                    case "name" -> Optional.of("alice");
                    default -> Optional.empty();
                };
            }
            return switch (expression) {
                case "id" -> Optional.of("2");
                case "name" -> Optional.of("bob");
                default -> Optional.empty();
            };
        });

        var exactDedupContext = DefaultEvaluationContext.builder()
                .dedupStrategy(DedupStrategy.exact())
                .build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, exactDedupContext)
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
        assertThat(iterations.get(1).getIndex(), is(1));
        assertThat(iterations.get(1).getValue("#"), is(Optional.of(1)));
    }

    @Test
    void givenNoneDedupWithDuplicates_whenEvaluated_thenDuplicatesPreserved() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(idField, nameField));

        var rec1 = createRecord("record-1");
        var rec2 = createRecord("record-2");

        // Both records evaluate to the same field values
        setupMocksWithPerRecordEval(Flux.just(rec1, rec2), rec -> expression -> switch (expression) {
            case "id" -> Optional.of("1");
            case "name" -> Optional.of("alice");
            default -> Optional.empty();
        });

        var noneContext = DefaultEvaluationContext.builder()
                .dedupStrategy(DedupStrategy.none())
                .build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, noneContext)
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
        assertThat(iterations.get(1).getIndex(), is(1));
        assertThat(iterations.get(1).getValue("#"), is(Optional.of(1)));
    }

    @Test
    void givenExactDedupWithPartialDuplicates_whenEvaluated_thenOnlyDuplicatesRemoved() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(idField, nameField));

        var rec1 = createRecord("record-1");
        var rec2 = createRecord("record-2");
        var rec3 = createRecord("record-3");

        // Records 1 and 3 produce identical values; record 2 is different
        setupMocksWithPerRecordEval(Flux.just(rec1, rec2, rec3), rec -> expression -> {
            if (rec == rec2.getSourceRecord()) {
                return switch (expression) {
                    case "id" -> Optional.of("2");
                    case "name" -> Optional.of("bob");
                    default -> Optional.empty();
                };
            }
            // rec1 and rec3 produce identical values
            return switch (expression) {
                case "id" -> Optional.of("1");
                case "name" -> Optional.of("alice");
                default -> Optional.empty();
            };
        });

        var exactDedupContext = DefaultEvaluationContext.builder()
                .dedupStrategy(DedupStrategy.exact())
                .build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, exactDedupContext)
                .collectList()
                .block();

        // Only 2 unique iterations: alice (from rec1) and bob (from rec2). rec3 is a duplicate of rec1.
        assertThat(iterations, hasSize(2));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("name"), is(Optional.of("alice")));
        assertThat(iterations.get(1).getIndex(), is(1));
        assertThat(iterations.get(1).getValue("#"), is(Optional.of(1)));
        assertThat(iterations.get(1).getValue("id"), is(Optional.of("2")));
        assertThat(iterations.get(1).getValue("name"), is(Optional.of("bob")));
    }

    @Test
    void givenExactDedupAfterJoinProducesDuplicates_whenEvaluated_thenDeduped() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("1");
            }
            return Optional.empty();
        };

        // Two child records both producing id=1 → duplicates after join extension
        var rec1 = createRecord("record-1");
        var rec2 = createRecord("record-2");
        setupChildView(Flux.just(rec1, rec2), exprEval);

        // One parent row: pid=1, city=NYC — matches both child records
        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var exactDedupContext = DefaultEvaluationContext.builder()
                .dedupStrategy(DedupStrategy.exact())
                .build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, exactDedupContext)
                .collectList()
                .block();

        // Both child records produce id=1, city=NYC, city.#=0 → exact dedup keeps only 1
        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.of("NYC")));
    }

    @Test
    void givenExactDedupWithLimit_whenEvaluated_thenLimitAppliedAfterDedup() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        var rec1 = createRecord("record-1");
        var rec2 = createRecord("record-2");
        var rec3 = createRecord("record-3");
        var rec4 = createRecord("record-4");

        // Records 1 & 3 produce "a", records 2 & 4 produce "b"
        setupMocksWithPerRecordEval(Flux.just(rec1, rec2, rec3, rec4), rec -> expression -> {
            if ("id".equals(expression)) {
                if (rec == rec1.getSourceRecord() || rec == rec3.getSourceRecord()) {
                    return Optional.of("a");
                }
                return Optional.of("b");
            }
            return Optional.empty();
        });

        // Exact dedup reduces 4 records to 2 unique ("a" and "b"), then limit takes 1
        var limitContext = DefaultEvaluationContext.builder()
                .dedupStrategy(DedupStrategy.exact())
                .limit(1L)
                .build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, limitContext)
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
    }

    @Test
    void givenExactDedupWithLeftJoinNoMatchAndIdenticalChildren_whenEvaluated_thenDuplicatesRemoved() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        // Two child records with the same id — neither matches the join
        var rec1 = createRecord("record-1");
        var rec2 = createRecord("record-2");
        setupChildViewPerRecord(
                Flux.just(rec1, rec2),
                rec -> expression -> "id".equals(expression) ? Optional.of("same") : Optional.empty());

        var parentView = setupParentView(List.of(Map.of("pid", "999", "city", "NYC")));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        lenient().when(cityJoinField.getReference()).thenReturn("city");
        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var exactDedupContext = DefaultEvaluationContext.builder()
                .dedupStrategy(DedupStrategy.exact())
                .build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, exactDedupContext)
                .collectList()
                .block();

        // Both children have id=same, city=absent, city.#=absent — exact dedup keeps only 1
        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("same")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.empty()));
    }

    @Test
    void givenExactDedupWithIdenticalIterableContent_whenEvaluated_thenDuplicatesRemoved() {
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord = new Object();

        // Two source records, each producing items=[sword] — identical iterations
        var record1 = createRecord("record-1");
        var record2 = createRecord("record-2");
        setupMocksWithPerRecordEval(Flux.just(record1, record2), rec -> expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord));
            }
            if ("type".equals(expression)) {
                return Optional.of("sword");
            }
            return Optional.empty();
        });

        var exactDedupContext = DefaultEvaluationContext.builder()
                .dedupStrategy(DedupStrategy.exact())
                .build();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, exactDedupContext)
                .collectList()
                .block();

        // Key fields: items.#, items.type, items.type.# — both records produce the same → dedup keeps 1
        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getIndex(), is(0));
        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
        assertThat(iterations.get(0).getValue("items.type"), is(Optional.of("sword")));
        assertThat(iterations.get(0).getValue("items.#"), is(Optional.of(0)));
    }

    // --- Natural datatype propagation tests ---

    @Test
    void givenResolverWithDatatypeMapper_whenEvaluated_thenNaturalDatatypesPropagated() {
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

        // Override getDatatypeMapperFactory to return xsd:string for "name"
        LogicalSourceResolver.DatatypeMapperFactory<Object> datatypeMapperFactory = sourceRecord -> expression -> {
            if ("name".equals(expression)) {
                return Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.STRING);
            }
            return Optional.empty();
        };
        when(resolver.getDatatypeMapperFactory()).thenReturn(Optional.of(datatypeMapperFactory));

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getValue("name"), is(Optional.of("alice")));
                    assertThat(
                            iteration.getNaturalDatatype("name"),
                            is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.STRING)));
                })
                .verifyComplete();
    }

    @Test
    void givenIndexKeys_whenEvaluated_thenIndexKeysHaveIntegerNaturalDatatype() {
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
                    assertThat(
                            iteration.getNaturalDatatype("#"),
                            is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.INTEGER)));
                    assertThat(
                            iteration.getNaturalDatatype("name.#"),
                            is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.INTEGER)));
                })
                .verifyComplete();
    }

    @Test
    void givenResolverWithoutDatatypeMapper_whenEvaluated_thenEmptyNaturalDatatypes() {
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

        // resolver.getDatatypeMapperFactory() returns Optional.empty() by default from @BeforeEach

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getValue("name"), is(Optional.of("alice")));
                    // No natural datatype for the field value (only index keys have one)
                    assertThat(iteration.getNaturalDatatype("name"), is(Optional.empty()));
                    // Index keys still have xsd:integer
                    assertThat(
                            iteration.getNaturalDatatype("name.#"),
                            is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.INTEGER)));
                })
                .verifyComplete();
    }

    @Test
    void givenViewOnView_whenEvaluated_thenNaturalDatatypesInheritedFromParent() {
        // Parent view: viewOn = logicalSource, field "name" → "alice" with xsd:string natural datatype
        var parentView = mock(LogicalView.class);
        var parentNameField = mockExpressionField("name");
        when(parentNameField.getReference()).thenReturn("name");
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentNameField));

        // Child view: viewOn = parentView, field "result" referencing parent's "name"
        var childResultField = mockExpressionField("result");
        when(childResultField.getReference()).thenReturn("name");
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childResultField));

        ExpressionEvaluation exprEval = expression -> {
            if ("name".equals(expression)) {
                return Optional.of("alice");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        // Parent resolver has a datatype mapper that returns xsd:string for "name"
        LogicalSourceResolver.DatatypeMapperFactory<Object> datatypeMapperFactory = sourceRecord -> expression -> {
            if ("name".equals(expression)) {
                return Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.STRING);
            }
            return Optional.empty();
        };
        when(resolver.getDatatypeMapperFactory()).thenReturn(Optional.of(datatypeMapperFactory));

        // Re-stub: setupMocks stubs logicalView.getViewOn() → logicalSource, but parentView needs that
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getIndex(), is(0));
                    assertThat(iteration.getValue("result"), is(Optional.of("alice")));
                    // Child references parent's "name" which has xsd:string natural datatype.
                    // The view-on-view DatatypeMapper delegates to parent's getNaturalDatatype("name"),
                    // which returns xsd:string. The child field "result" references "name", so the
                    // datatype mapper is applied with expression "name" and returns xsd:string.
                    assertThat(
                            iteration.getNaturalDatatype("result"),
                            is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.STRING)));
                })
                .verifyComplete();
    }

    @Test
    void givenIterableField_whenEvaluated_thenIterableIndexKeyHasIntegerNaturalDatatype() {
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord1 = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord1));
            }
            return Optional.empty();
        };

        Function<Object, ExpressionEvaluation> perRecordFactory = rec -> expression -> {
            if ("type".equals(expression) && rec == subRecord1) {
                return Optional.of("sword");
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

        assertThat(iterations, hasSize(1));
        // Iterable index key items.# has xsd:integer natural datatype
        assertThat(
                iterations.get(0).getNaturalDatatype("items.#"),
                is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.INTEGER)));
        // Expression field index key items.type.# has xsd:integer natural datatype
        assertThat(
                iterations.get(0).getNaturalDatatype("items.type.#"),
                is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.INTEGER)));
    }

    @Test
    void givenIterableFieldWithDatatypeMapper_whenEvaluated_thenNestedFieldNaturalDatatypePropagated() {
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");

        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(logicalView.getFields()).thenReturn(Set.of(itemsIterable));

        var subRecord1 = new Object();

        ExpressionEvaluation rootExprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(subRecord1));
            }
            return Optional.empty();
        };

        Function<Object, ExpressionEvaluation> perRecordFactory = rec -> expression -> {
            if ("type".equals(expression) && rec == subRecord1) {
                return Optional.of("sword");
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

        // Override DatatypeMapperFactory: map "type" → xsd:string
        LogicalSourceResolver.DatatypeMapperFactory<Object> datatypeMapperFactory =
                sourceRecord -> expression -> "type".equals(expression)
                        ? Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.STRING)
                        : Optional.empty();
        when(resolver.getDatatypeMapperFactory()).thenReturn(Optional.of(datatypeMapperFactory));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        // Nested expression field items.type carries xsd:string from the inherited mapper
        assertThat(
                iterations.get(0).getNaturalDatatype("items.type"),
                is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.STRING)));
        // Index keys remain xsd:integer
        assertThat(
                iterations.get(0).getNaturalDatatype("items.#"),
                is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.INTEGER)));
    }

    @Test
    void givenJoinFieldWithParentNaturalDatatype_whenEvaluated_thenJoinFieldNaturalDatatypePropagated() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("1") : Optional.empty();

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        // Set up parent view with a resolver that maps "city" → xsd:string
        var parentView = mock(LogicalView.class);
        var parentLogicalSource = mock(LogicalSource.class);
        var parentSource = mock(Source.class);

        when(parentView.getViewOn()).thenReturn(parentLogicalSource);
        when(parentLogicalSource.getSource()).thenReturn(parentSource);

        LogicalSourceResolver<Object> parentResolver = mock();

        var parentRow = Map.<String, Object>of("pid", "1", "city", "NYC");
        var parentRecords = List.of(LogicalSourceRecord.of(parentLogicalSource, (Object) parentRow));

        when(parentResolver.getLogicalSourceRecords(anySet(), anyMap()))
                .thenReturn(rs -> Flux.fromIterable(parentRecords));
        LogicalSourceResolver.ExpressionEvaluationFactory<Object> parentExprEvalFactory =
                rec2 -> expression -> Optional.ofNullable(((Map<?, ?>) rec2).get(expression));
        when(parentResolver.getExpressionEvaluationFactory()).thenReturn(parentExprEvalFactory);

        // Parent resolver maps "city" → xsd:string
        LogicalSourceResolver.DatatypeMapperFactory<Object> parentDatatypeMapperFactory =
                sourceRecord -> expression -> "city".equals(expression)
                        ? Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.STRING)
                        : Optional.empty();
        when(parentResolver.getDatatypeMapperFactory()).thenReturn(Optional.of(parentDatatypeMapperFactory));

        var pidField = mockExpressionField("pid");
        when(pidField.getReference()).thenReturn("pid");
        var cityField = mockExpressionField("city");
        when(cityField.getReference()).thenReturn("city");
        when(parentView.getFields()).thenReturn(Set.of(pidField, cityField));

        parentBindings.add(new ViewResolverBinding(parentLogicalSource, parentSource, parentResolver));
        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");
        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("city"), is(Optional.of("NYC")));
        // Join field "city" carries xsd:string from parent's natural datatype
        assertThat(
                iterations.get(0).getNaturalDatatype("city"),
                is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.STRING)));
        // Join field index key is xsd:integer
        assertThat(
                iterations.get(0).getNaturalDatatype("city.#"),
                is(Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.INTEGER)));
    }

    @Test
    void givenTemplateFieldWithDatatypeMapper_whenEvaluated_thenNoNaturalDatatypeForTemplateField() {
        var labelField = mockExpressionField("label");
        var seg1 = mock(CarmlTemplate.TextSegment.class);
        when(seg1.getValue()).thenReturn("item-");
        var seg2 = mock(CarmlTemplate.ExpressionSegment.class);
        when(seg2.getValue()).thenReturn("id");
        var template = mock(Template.class);
        when(template.getSegments()).thenReturn(List.of(seg1, seg2));
        when(labelField.getTemplate()).thenReturn(template);
        when(labelField.getReference()).thenReturn(null);
        when(logicalView.getFields()).thenReturn(Set.of(labelField));

        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("42") : Optional.empty();

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        // Mapper returns xsd:string for any expression — should not leak into template field
        LogicalSourceResolver.DatatypeMapperFactory<Object> datatypeMapperFactory =
                sourceRecord -> expression -> Optional.of(org.eclipse.rdf4j.model.vocabulary.XSD.STRING);
        when(resolver.getDatatypeMapperFactory()).thenReturn(Optional.of(datatypeMapperFactory));

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getValue("label"), is(Optional.of("item-42")));
                    // Template fields must not carry a natural datatype
                    assertThat(iteration.getNaturalDatatype("label"), is(Optional.empty()));
                })
                .verifyComplete();
    }

    // --- Non-referenceable key validation tests ---

    @Test
    void givenViewOnViewReferencingItKey_whenEvaluated_thenThrowsException() {
        var parentView = mock(LogicalView.class);
        var parentNameField = mockExpressionField("name");
        when(parentNameField.getReference()).thenReturn("name");
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentNameField));

        // Child references the root iterable record key "<it>"
        var childField = mockExpressionField("result");
        when(childField.getReference()).thenReturn("<it>");
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childField));

        ExpressionEvaluation exprEval = expression -> {
            if ("name".equals(expression)) {
                return Optional.of("alice");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .expectErrorSatisfies(error -> {
                    assertThat(
                            error, org.hamcrest.Matchers.instanceOf(ViewIterationExpressionEvaluationException.class));
                    assertThat(error.getMessage(), containsString("Reference to root iterable record key '<it>'"));
                    assertThat(error.getMessage(), containsString("is not a referenceable key in a logical view"));
                })
                .verify();
    }

    @Test
    void givenViewOnViewReferencingIterableRecordKey_whenEvaluated_thenThrowsException() {
        var parentView = mock(LogicalView.class);

        // Parent has an iterable field "items" containing nested expression field "type"
        var typeField = mockExpressionField("type");
        when(typeField.getReference()).thenReturn("type");
        var itemsIterable = mockIterableField("items", "$.items[*]", Set.of(typeField));
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(itemsIterable));

        // Child references "items" directly — the iterable record key, not a nested field
        var childField = mockExpressionField("result");
        when(childField.getReference()).thenReturn("items");
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childField));

        ExpressionEvaluation exprEval = expression -> {
            if ("$.items[*]".equals(expression)) {
                return Optional.of(List.of(new Object()));
            }
            if ("type".equals(expression)) {
                return Optional.of("sword");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .expectErrorSatisfies(error -> {
                    assertThat(
                            error, org.hamcrest.Matchers.instanceOf(ViewIterationExpressionEvaluationException.class));
                    assertThat(error.getMessage(), containsString("Reference to iterable record key 'items'"));
                    assertThat(error.getMessage(), containsString("Use its nested field names instead"));
                })
                .verify();
    }

    @Test
    void givenViewOnViewWithValidKeys_whenEvaluated_thenSucceeds() {
        // Positive regression test: child references valid keys from parent (expression field and index key)
        var parentView = mock(LogicalView.class);
        var parentNameField = mockExpressionField("name");
        when(parentNameField.getReference()).thenReturn("name");
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(Set.of(parentNameField));

        var childResultField = mockExpressionField("result");
        when(childResultField.getReference()).thenReturn("name");
        var childIndexField = mockExpressionField("idx");
        when(childIndexField.getReference()).thenReturn("name.#");
        when(logicalView.getViewOn()).thenReturn(parentView);
        when(logicalView.getFields()).thenReturn(Set.of(childResultField, childIndexField));

        ExpressionEvaluation exprEval = expression -> {
            if ("name".equals(expression)) {
                return Optional.of("alice");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(logicalView.getViewOn()).thenReturn(parentView);

        StepVerifier.create(evaluator.evaluate(logicalView, sourceResolver, EvaluationContext.defaults()))
                .assertNext(iteration -> {
                    assertThat(iteration.getValue("result"), is(Optional.of("alice")));
                    assertThat(iteration.getValue("idx"), is(Optional.of(0)));
                })
                .verifyComplete();
    }

    // --- Join optimization tests ---

    private ForeignKeyAnnotation mockForeignKeyAnnotation(LogicalView targetView, List<Field> targetFields) {
        var fk = mock(ForeignKeyAnnotation.class);
        when(fk.getTargetView()).thenReturn(targetView);
        when(fk.getTargetFields()).thenReturn(targetFields);
        return fk;
    }

    private PrimaryKeyAnnotation mockPrimaryKeyAnnotation(List<Field> onFields) {
        var pk = mock(PrimaryKeyAnnotation.class);
        when(pk.getOnFields()).thenReturn(onFields);
        return pk;
    }

    private NotNullAnnotation mockNotNullAnnotation(List<Field> onFields) {
        var nn = mock(NotNullAnnotation.class);
        when(nn.getOnFields()).thenReturn(onFields);
        return nn;
    }

    private UniqueAnnotation mockUniqueAnnotation(List<Field> onFields) {
        var u = mock(UniqueAnnotation.class);
        when(u.getOnFields()).thenReturn(onFields);
        return u;
    }

    private Field mockField(String name) {
        var f = mock(Field.class);
        lenient().when(f.getFieldName()).thenReturn(name);
        return f;
    }

    @Test
    void givenPrimaryKeyOnParentConditionFields_whenLeftJoinEvaluated_thenSingleMatchPerChild() {
        // Child: id=1, name=alice
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(idField, nameField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "id" -> Optional.of("1");
            case "name" -> Optional.of("alice");
            default -> Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        // Parent: two rows with same join key pid=1 but different cities
        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC"), Map.of("pid", "1", "city", "LA")));

        // PK annotation on parent for "pid" field
        var pkField = mockField("pid");
        var pk = mockPrimaryKeyAnnotation(List.of(pkField));
        when(parentView.getStructuralAnnotations()).thenReturn(Set.of(pk));

        // No annotations on child
        when(logicalView.getStructuralAnnotations()).thenReturn(Set.of());

        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // With PK on parent "pid", single-match optimization kicks in → only 1 result per child
        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("name"), is(Optional.of("alice")));
        // City should be present (either NYC or LA, just one)
        assertThat(iterations.get(0).getValue("city").isPresent(), is(true));
    }

    @Test
    void givenFkAndUniqueNotNullOnParent_whenLeftJoinEvaluated_thenSingleMatchPerChild() {
        // Child: id=1
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("1");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        // Parent: two rows with same pid
        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC"), Map.of("pid", "1", "city", "LA")));

        // FK on child referencing parent view, target fields = ["pid"]
        var fkTargetField = mockField("pid");
        var fk = mockForeignKeyAnnotation(parentView, List.of(fkTargetField));

        // Unique+NotNull on parent for "pid" field
        var parentPidField = mockField("pid");
        var unique = mockUniqueAnnotation(List.of(parentPidField));
        var notNull = mockNotNullAnnotation(List.of(parentPidField));
        when(parentView.getStructuralAnnotations()).thenReturn(Set.of(unique, notNull));

        when(logicalView.getStructuralAnnotations()).thenReturn(Set.of(fk));

        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // FK + Unique+NotNull on parent → single-match optimization → only 1 result
        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("city").isPresent(), is(true));
    }

    @Test
    void givenFkAndProjectedFieldsExcludeJoinFields_whenEvaluated_thenJoinEliminated() {
        // Child: id=1, name=alice
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(idField, nameField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "id" -> Optional.of("1");
            case "name" -> Optional.of("alice");
            default -> Optional.empty();
        };

        var rec = createRecord("record-1");
        setupMocks(Flux.just(rec), exprEval);

        // Bare parent view mock — the join is eliminated so the parent is never evaluated
        var parentView = mock(LogicalView.class);

        // FK on child referencing parent view
        var fkTargetField = mockField("pid");
        var fk = mockForeignKeyAnnotation(parentView, List.of(fkTargetField));
        when(logicalView.getStructuralAnnotations()).thenReturn(Set.of(fk));

        var cityJoinField = mockExpressionField("city");
        lenient().when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        // Projected fields exclude "city" (the join field) — only project "id" and "name"
        var context = EvaluationContext.withProjectedFields(Set.of("id", "name"));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, context)
                .collectList()
                .block();

        // Join eliminated → result is just the child rows without join fields
        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("name"), is(Optional.of("alice")));
        // "city" should not be present since join was eliminated
        assertThat(iterations.get(0).getValue("city"), is(Optional.empty()));
    }

    @Test
    void givenNotNullOnChildConditionFields_whenLeftJoinNoMatch_thenChildDropped() {
        // Child has id=99, no parent matches
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> {
            if ("id".equals(expression)) {
                return Optional.of("99");
            }
            return Optional.empty();
        };

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));

        // NotNull annotation on child for "id" field (the join condition field)
        var nnField = mockField("id");
        var nn = mockNotNullAnnotation(List.of(nnField));
        when(logicalView.getStructuralAnnotations()).thenReturn(Set.of(nn));
        lenient().when(parentView.getStructuralAnnotations()).thenReturn(Set.of());

        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        lenient().when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // LEFT→INNER conversion: NotNull on child condition field → unmatched child is dropped
        assertThat(iterations, hasSize(0));
    }

    @Test
    void givenNoAnnotations_whenLeftJoinMultiMatch_thenAllMatchesPreservedAndUnmatchedPreserved() {
        // Two child records: id=1, id=99
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        setupChildViewPerRecord(Flux.just(createRecord("rec-1"), createRecord("rec-2")), rec -> {
            if ("rec-1".equals(rec)) {
                return expression -> {
                    if ("id".equals(expression)) {
                        return Optional.of("1");
                    }
                    return Optional.empty();
                };
            }
            return expression -> {
                if ("id".equals(expression)) {
                    return Optional.of("99");
                }
                return Optional.empty();
            };
        });

        // Two parent rows with same pid=1
        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC"), Map.of("pid", "1", "city", "LA")));

        // No annotations on either view
        when(logicalView.getStructuralAnnotations()).thenReturn(Set.of());
        when(parentView.getStructuralAnnotations()).thenReturn(Set.of());

        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // No annotations → no optimizations:
        // child id=1 matches 2 parents → 2 rows
        // child id=99 matches 0 parents → 1 row (left join preserves)
        assertThat(iterations, hasSize(3));

        var matchedCities = iterations.stream()
                .filter(it -> it.getValue("id").orElseThrow().equals("1"))
                .map(it -> it.getValue("city").orElseThrow().toString())
                .toList();
        assertThat(matchedCities, containsInAnyOrder("NYC", "LA"));

        var unmatchedRow = iterations.stream()
                .filter(it -> it.getValue("id").orElseThrow().equals("99"))
                .toList();
        assertThat(unmatchedRow, hasSize(1));
        assertThat(unmatchedRow.get(0).getValue("city"), is(Optional.empty()));
    }

    @Test
    void givenPrimaryKeyOnSupersetOfConditionFields_whenLeftJoinEvaluated_thenMultipleMatchesReturned() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("1") : Optional.empty();

        var rec = createRecord("record-1");
        setupChildView(Flux.just(rec), exprEval);

        // Parent: two rows, same pid but different regions
        var parentView = setupParentView(List.of(
                Map.of("pid", "1", "region", "EU", "city", "Paris"),
                Map.of("pid", "1", "region", "US", "city", "NYC")));

        // PK on composite {pid, region} — superset of condition field {pid}
        var pk = mockPrimaryKeyAnnotation(List.of(mockField("pid"), mockField("region")));
        when(parentView.getStructuralAnnotations()).thenReturn(Set.of(pk));
        when(logicalView.getStructuralAnnotations()).thenReturn(Set.of());

        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // Composite PK does NOT exactly cover the condition fields → no singleMatch → 2 results
        assertThat(iterations, hasSize(2));
        var cities = iterations.stream()
                .map(it -> it.getValue("city").orElseThrow().toString())
                .toList();
        assertThat(cities, containsInAnyOrder("Paris", "NYC"));
    }

    @Test
    void givenFkButEmptyProjectedFields_whenEvaluated_thenJoinNotEliminated() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("1") : Optional.empty();
        setupChildView(Flux.just(createRecord("record-1")), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));

        var fk = mockForeignKeyAnnotation(parentView, List.of(mockField("pid")));
        when(logicalView.getStructuralAnnotations()).thenReturn(Set.of(fk));
        when(parentView.getStructuralAnnotations()).thenReturn(Set.of());

        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        // EvaluationContext.defaults() → projectedFields = empty set → all fields projected
        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // Empty projectedFields → join NOT eliminated → city is present
        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("city"), is(Optional.of("NYC")));
    }

    @Test
    void givenNotNullOnChildConditionFields_whenLeftJoinMatches_thenChildKeptWithJoinFields() {
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> "id".equals(expression) ? Optional.of("1") : Optional.empty();
        setupChildView(Flux.just(createRecord("record-1")), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "city", "NYC")));

        var nn = mockNotNullAnnotation(List.of(mockField("id")));
        when(logicalView.getStructuralAnnotations()).thenReturn(Set.of(nn));
        lenient().when(parentView.getStructuralAnnotations()).thenReturn(Set.of());

        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        when(cityJoinField.getReference()).thenReturn("city");

        var joinCondition = mockJoinCondition("id", "pid");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCondition), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // LEFT→INNER conversion: child matched → kept and extended with join field
        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("id"), is(Optional.of("1")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.of("NYC")));
    }

    @Test
    void givenNotNullOnOnlyOneOfTwoConditionFields_whenLeftJoinNoMatch_thenChildPreservedAsLeft() {
        var aField = mockExpressionField("a");
        when(aField.getReference()).thenReturn("a");
        var bField = mockExpressionField("b");
        when(bField.getReference()).thenReturn("b");
        when(logicalView.getFields()).thenReturn(Set.of(aField, bField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "a" -> Optional.of("99"); // no match
            case "b" -> Optional.of("EU");
            default -> Optional.empty();
        };
        setupChildView(Flux.just(createRecord("record-1")), exprEval);

        var parentView = setupParentView(List.of(Map.of("pid", "1", "region", "EU", "city", "Paris")));

        // NotNull only on "a", NOT on "b" → partial coverage → no conversion
        var nn = mockNotNullAnnotation(List.of(mockField("a")));
        when(logicalView.getStructuralAnnotations()).thenReturn(Set.of(nn));
        lenient().when(parentView.getStructuralAnnotations()).thenReturn(Set.of());

        buildJoinEvaluator();

        var cityJoinField = mockExpressionField("city");
        lenient().when(cityJoinField.getReference()).thenReturn("city");

        var cond1 = mockJoinCondition("a", "pid");
        var cond2 = mockJoinCondition("b", "region");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(cond1, cond2), Set.of(cityJoinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        // Partial NotNull → conversion NOT triggered → left join preserves unmatched child
        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("a"), is(Optional.of("99")));
        assertThat(iterations.get(0).getValue("city"), is(Optional.empty()));
    }

    // --- Dedup key narrowing tests ---

    @Test
    void givenProjectedFields_whenDedupApplied_thenNarrowedToProjectedFieldsOnly() {
        var fieldA = mockExpressionField("a");
        when(fieldA.getReference()).thenReturn("$.a");
        var fieldB = mockExpressionField("b");
        when(fieldB.getReference()).thenReturn("$.b");
        when(logicalView.getFields()).thenReturn(Set.of(fieldA, fieldB));

        // Two records: same "a" value, different "b" value
        var record1 = createRecord("record-1");
        var record2 = createRecord("record-2");

        setupMocksWithPerRecordEval(Flux.just(record1, record2), rec -> {
            if (rec == record1.getSourceRecord()) {
                return expression -> switch (expression) {
                    case "$.a" -> Optional.of("x");
                    case "$.b" -> Optional.of("1");
                    default -> Optional.empty();
                };
            }
            if (rec == record2.getSourceRecord()) {
                return expression -> switch (expression) {
                    case "$.a" -> Optional.of("x");
                    case "$.b" -> Optional.of("2");
                    default -> Optional.empty();
                };
            }
            return expression -> Optional.empty();
        });

        // Projected only "a" → dedup key is narrowed to {a, a.#} → duplicate on "a" is eliminated
        var context = EvaluationContext.of(Set.of("a"), DedupStrategy.exact(), null);

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, context)
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("a"), is(Optional.of("x")));
    }

    @Test
    void givenEmptyProjectedFields_whenDedupApplied_thenUsesAllFields() {
        var fieldA = mockExpressionField("a");
        when(fieldA.getReference()).thenReturn("$.a");
        var fieldB = mockExpressionField("b");
        when(fieldB.getReference()).thenReturn("$.b");
        when(logicalView.getFields()).thenReturn(Set.of(fieldA, fieldB));

        // Two records: same "a" value, different "b" value
        var record1 = createRecord("record-1");
        var record2 = createRecord("record-2");

        setupMocksWithPerRecordEval(Flux.just(record1, record2), rec -> {
            if (rec == record1.getSourceRecord()) {
                return expression -> switch (expression) {
                    case "$.a" -> Optional.of("x");
                    case "$.b" -> Optional.of("1");
                    default -> Optional.empty();
                };
            }
            if (rec == record2.getSourceRecord()) {
                return expression -> switch (expression) {
                    case "$.a" -> Optional.of("x");
                    case "$.b" -> Optional.of("2");
                    default -> Optional.empty();
                };
            }
            return expression -> Optional.empty();
        });

        // Empty projected → dedup key uses all fields → different "b" means both rows kept
        var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, context)
                .collectList()
                .block();

        assertThat(iterations, hasSize(2));
    }

    // --- Same-source join optimization tests ---

    /**
     * Creates a parent view backed by the SAME logicalSource as the child. This enables the
     * same-source optimization: parent iterations are derived from child data instead of
     * re-reading the source.
     */
    private LogicalView setupSameSourceParentView(Set<Field> parentFields) {
        var parentView = mock(LogicalView.class);
        when(parentView.getViewOn()).thenReturn(logicalSource);
        when(parentView.getFields()).thenReturn(parentFields);
        lenient().when(parentView.getLeftJoins()).thenReturn(null);
        lenient().when(parentView.getInnerJoins()).thenReturn(null);
        return parentView;
    }

    @Test
    void givenSameSourceParentJoin_whenEvaluated_thenSourceReadOnlyOnce() {
        // Child view: fields id, name
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(idField, nameField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "id" -> Optional.of("1");
            case "name" -> Optional.of("alice");
            default -> Optional.empty();
        };

        var rec = createRecord("rec");

        // Parent view: same source, fields "id" and "name" (subset of child)
        var parentIdField = mockExpressionField("id");
        lenient().when(parentIdField.getReference()).thenReturn("id");
        var parentNameField = mockExpressionField("name");
        lenient().when(parentNameField.getReference()).thenReturn("name");
        var parentView = setupSameSourceParentView(Set.of(parentIdField, parentNameField));

        var joinCond = mockJoinCondition("id", "id");
        var joinField = mockExpressionField("_ref0.name");
        when(joinField.getReference()).thenReturn("name");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        // Use setupMocks (not buildJoinEvaluator) — parent is derived from child, no parent
        // resolver needed. This also verifies the optimization activates correctly.
        setupMocks(Flux.just(rec), exprEval);

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
        assertThat(iterations.get(0).getValue("name"), is(Optional.of("alice")));
        assertThat(iterations.get(0).getValue("_ref0.name"), is(Optional.of("alice")));

        // Source resolver called only once (for child) — parent derived from child data
        verify(resolver, times(1)).getLogicalSourceRecords(anySet(), anyMap());
    }

    @Test
    void givenParentFieldNotInChild_whenEvaluated_thenOptimizationSkippedAndSourceReadTwice() {
        // Child view: field "id" only
        var idField = mockExpressionField("id");
        when(idField.getReference()).thenReturn("id");
        when(logicalView.getFields()).thenReturn(Set.of(idField));

        ExpressionEvaluation exprEval = expression -> switch (expression) {
            case "id" -> Optional.of("1");
            case "extra" -> Optional.of("x");
            default -> Optional.empty();
        };

        var rec = createRecord("rec");
        setupChildView(Flux.just(rec), exprEval);

        // Parent view: same source but has "extra" field NOT in child — optimization skipped
        var parentIdField = mockExpressionField("id");
        when(parentIdField.getReference()).thenReturn("id");
        var parentExtraField = mockExpressionField("extra");
        when(parentExtraField.getReference()).thenReturn("extra");
        var parentView = setupSameSourceParentView(Set.of(parentIdField, parentExtraField));

        // Need parent resolver since optimization won't apply
        parentBindings.add(new ViewResolverBinding(logicalSource, source, resolver));

        var joinCond = mockJoinCondition("id", "id");
        var joinField = mockExpressionField("_ref0.extra");
        when(joinField.getReference()).thenReturn("extra");
        var lvJoin = mockLogicalViewJoin(parentView, Set.of(joinCond), Set.of(joinField));
        when(logicalView.getLeftJoins()).thenReturn(Set.of(lvJoin));

        buildJoinEvaluator();

        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));

        // Source resolver should be called twice — optimization skipped because "extra" not in child
        verify(resolver, times(2)).getLogicalSourceRecords(anySet(), anyMap());
    }

    @Test
    void givenViewOnViewChild_whenEvaluated_thenSameSourceOptimizationNotActivated() {
        // Child viewOn is a LogicalView (not LogicalSource) — optimization should not apply
        var innerView = mock(LogicalView.class);
        when(innerView.getViewOn()).thenReturn(logicalSource);
        when(logicalSource.getSource()).thenReturn(source);

        var nameField = mockExpressionField("name");
        when(nameField.getReference()).thenReturn("name");
        when(innerView.getFields()).thenReturn(Set.of(nameField));

        // Outer view wraps inner view (viewOn is a LogicalView, not LogicalSource)
        when(logicalView.getViewOn()).thenReturn(innerView);

        var outerNameField = mockExpressionField("name");
        when(outerNameField.getReference()).thenReturn("name");
        when(logicalView.getFields()).thenReturn(Set.of(outerNameField));

        ExpressionEvaluation exprEval =
                expression -> "name".equals(expression) ? Optional.of("alice") : Optional.empty();

        when(resolver.getLogicalSourceRecords(anySet(), anyMap()))
                .thenReturn(rs -> Flux.just(LogicalSourceRecord.of(logicalSource, (Object) "rec")));
        when(resolver.getExpressionEvaluationFactory()).thenReturn(rec -> exprEval);

        buildJoinEvaluator();

        // Succeeds without error — view-on-view guard prevents same-source optimization
        var iterations = evaluator
                .evaluate(logicalView, sourceResolver, EvaluationContext.defaults())
                .collectList()
                .block();

        assertThat(iterations, hasSize(1));
    }
}
