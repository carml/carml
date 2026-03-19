package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.model.ExpressionField;
import io.carml.model.IterableField;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.Source;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class SourceRecordCacheTest {

    @Nested
    class GetOrResolve {

        @Mock
        private Source source;

        @Test
        void givenRealCache_whenCalledTwice_thenResolverInvokedOnce() {
            var cache = SourceRecordCache.create();
            var callCount = new AtomicInteger(0);
            var expected =
                    new SourceRecordCache.CachedRecords(List.of(), mockExprEvalFactory(), mockDatatypeMapperFactory());

            var result1 = cache.getOrResolve(source, () -> {
                        callCount.incrementAndGet();
                        return Mono.just(expected);
                    })
                    .block();
            var result2 = cache.getOrResolve(source, () -> {
                        callCount.incrementAndGet();
                        return Mono.just(expected);
                    })
                    .block();

            assertThat(callCount.get(), is(1));
            assertThat(result1, is(sameInstance(result2)));
        }

        @Test
        void givenRealCache_whenDifferentSources_thenResolverInvokedPerSource() {
            var cache = SourceRecordCache.create();
            var source2 = mock(Source.class);
            var callCount = new AtomicInteger(0);

            cache.getOrResolve(source, () -> {
                        callCount.incrementAndGet();
                        return Mono.just(new SourceRecordCache.CachedRecords(
                                List.of(), mockExprEvalFactory(), mockDatatypeMapperFactory()));
                    })
                    .block();
            cache.getOrResolve(source2, () -> {
                        callCount.incrementAndGet();
                        return Mono.just(new SourceRecordCache.CachedRecords(
                                List.of(), mockExprEvalFactory(), mockDatatypeMapperFactory()));
                    })
                    .block();

            assertThat(callCount.get(), is(2));
        }

        @Test
        void givenNoopCache_thenReturnsEmpty() {
            var cache = SourceRecordCache.noop();
            var result = cache.getOrResolve(
                            source,
                            () -> Mono.just(new SourceRecordCache.CachedRecords(
                                    List.of(), mockExprEvalFactory(), mockDatatypeMapperFactory())))
                    .blockOptional();

            assertThat(result.isEmpty(), is(true));
        }
    }

    @Nested
    class IsActive {

        @Test
        void givenCreate_thenTrue() {
            assertThat(SourceRecordCache.create().isActive(), is(true));
        }

        @Test
        void givenNoop_thenFalse() {
            assertThat(SourceRecordCache.noop().isActive(), is(false));
        }
    }

    @Nested
    class CollectLogicalSourceInfo {

        @Mock
        private Source source;

        @Mock
        private Source source2;

        @Mock
        private LogicalSource logicalSource;

        @Mock
        private LogicalSource logicalSource2;

        @Test
        void givenSingleView_thenCollectsLogicalSourceAndExpressions() {
            var view = mockView(logicalSource);
            var field = mockExpressionField("name", "$.name");
            when(view.getFields()).thenReturn(Set.of(field));

            when(logicalSource.getSource()).thenReturn(source);

            var logicalSourcesPerSource = new LinkedHashMap<Source, Set<LogicalSource>>();
            var expressionsPerLogicalSource = new LinkedHashMap<LogicalSource, Set<String>>();

            SourceRecordCache.collectLogicalSourceInfo(
                    Set.of(view), logicalSourcesPerSource, expressionsPerLogicalSource);

            assertThat(logicalSourcesPerSource.get(source), contains(logicalSource));
            assertThat(expressionsPerLogicalSource.get(logicalSource), contains("$.name"));
        }

        @Test
        void givenTwoViewsOnSameSource_thenBothLogicalSourcesCollected() {
            when(logicalSource.getSource()).thenReturn(source);
            when(logicalSource2.getSource()).thenReturn(source);

            var nameField = mockExpressionField("name", "$.name");
            var ageField = mockExpressionField("age", "$.age");

            var view1 = mockView(logicalSource);
            when(view1.getFields()).thenReturn(Set.of(nameField));

            var view2 = mockView(logicalSource2);
            when(view2.getFields()).thenReturn(Set.of(ageField));

            var logicalSourcesPerSource = new LinkedHashMap<Source, Set<LogicalSource>>();
            var expressionsPerLogicalSource = new LinkedHashMap<LogicalSource, Set<String>>();

            SourceRecordCache.collectLogicalSourceInfo(
                    Set.of(view1, view2), logicalSourcesPerSource, expressionsPerLogicalSource);

            assertThat(logicalSourcesPerSource.get(source), hasSize(2));
        }

        @Test
        void givenViewWithJoinParent_thenCollectsParentLogicalSource() {
            when(logicalSource.getSource()).thenReturn(source);
            when(logicalSource2.getSource()).thenReturn(source2);

            // Child view on logicalSource
            var childView = mockView(logicalSource);
            var childField = mockExpressionField("childField", "$.child");
            when(childView.getFields()).thenReturn(Set.of(childField));

            // Parent view on logicalSource2
            var parentView = mockView(logicalSource2);
            var parentField = mockExpressionField("parentField", "$.parent");
            when(parentView.getFields()).thenReturn(Set.of(parentField));

            // Left join from child to parent
            var join = mockJoin(parentView);
            when(childView.getLeftJoins()).thenReturn(Set.of(join));

            var logicalSourcesPerSource = new LinkedHashMap<Source, Set<LogicalSource>>();
            var expressionsPerLogicalSource = new LinkedHashMap<LogicalSource, Set<String>>();

            SourceRecordCache.collectLogicalSourceInfo(
                    Set.of(childView), logicalSourcesPerSource, expressionsPerLogicalSource);

            // Both sources should be collected
            assertThat(logicalSourcesPerSource.get(source), contains(logicalSource));
            assertThat(logicalSourcesPerSource.get(source2), contains(logicalSource2));
        }

        @Test
        void givenDiamondDependency_thenViewVisitedOnce() {
            when(logicalSource.getSource()).thenReturn(source);
            when(logicalSource2.getSource()).thenReturn(source2);

            var parentField = mockExpressionField("parentField", "$.parent");
            var c1Field = mockExpressionField("c1", "$.c1");
            var c2Field = mockExpressionField("c2", "$.c2");

            // Shared parent view
            var parentView = mockView(logicalSource2);
            when(parentView.getFields()).thenReturn(Set.of(parentField));

            // Two child views both joining to the same parent
            var join1 = mockJoin(parentView);
            var join2 = mockJoin(parentView);

            var child1 = mockView(logicalSource);
            when(child1.getFields()).thenReturn(Set.of(c1Field));
            when(child1.getLeftJoins()).thenReturn(Set.of(join1));

            var child2 = mockView(logicalSource);
            when(child2.getFields()).thenReturn(Set.of(c2Field));
            when(child2.getLeftJoins()).thenReturn(Set.of(join2));

            var logicalSourcesPerSource = new LinkedHashMap<Source, Set<LogicalSource>>();
            var expressionsPerLogicalSource = new LinkedHashMap<LogicalSource, Set<String>>();

            SourceRecordCache.collectLogicalSourceInfo(
                    Set.of(child1, child2), logicalSourcesPerSource, expressionsPerLogicalSource);

            // Parent expressions collected only once (not duplicated)
            assertThat(expressionsPerLogicalSource.get(logicalSource2), contains("$.parent"));
        }

        @Test
        void givenViewWithJoinConditions_thenCollectsChildExpressions() {
            when(logicalSource.getSource()).thenReturn(source);
            when(logicalSource2.getSource()).thenReturn(source2);

            var nameField = mockExpressionField("name", "$.name");

            var childView = mockView(logicalSource);
            when(childView.getFields()).thenReturn(Set.of(nameField));

            var parentView = mockView(logicalSource2);
            when(parentView.getFields()).thenReturn(Set.of());

            // Join with a child-side condition expression
            var join = mockJoin(parentView);
            var childMap = mock(io.carml.model.ChildMap.class);
            when(childMap.getExpressionMapExpressionSet()).thenReturn(Set.of("$.joinKey"));
            var joinCondition = mock(Join.class);
            when(joinCondition.getChildMap()).thenReturn(childMap);
            when(join.getJoinConditions()).thenReturn(Set.of(joinCondition));

            when(childView.getLeftJoins()).thenReturn(Set.of(join));

            var logicalSourcesPerSource = new LinkedHashMap<Source, Set<LogicalSource>>();
            var expressionsPerLogicalSource = new LinkedHashMap<LogicalSource, Set<String>>();

            SourceRecordCache.collectLogicalSourceInfo(
                    Set.of(childView), logicalSourcesPerSource, expressionsPerLogicalSource);

            // Child expressions should include both field and join condition expressions
            var childExprs = expressionsPerLogicalSource.get(logicalSource);
            assertThat(childExprs.contains("$.name"), is(true));
            assertThat(childExprs.contains("$.joinKey"), is(true));
        }

        @Test
        void givenViewWithIterableFieldChildren_thenCollectsNestedExpressions() {
            when(logicalSource.getSource()).thenReturn(source);

            var childField = mockExpressionField("item", "$.item");
            var iterableField = mock(IterableField.class);
            lenient().when(iterableField.getFieldName()).thenReturn("items");
            when(iterableField.getFields()).thenReturn(Set.of(childField));

            var view = mockView(logicalSource);
            when(view.getFields()).thenReturn(Set.of(iterableField));

            var logicalSourcesPerSource = new LinkedHashMap<Source, Set<LogicalSource>>();
            var expressionsPerLogicalSource = new LinkedHashMap<LogicalSource, Set<String>>();

            SourceRecordCache.collectLogicalSourceInfo(
                    Set.of(view), logicalSourcesPerSource, expressionsPerLogicalSource);

            assertThat(expressionsPerLogicalSource.get(logicalSource).contains("$.item"), is(true));
        }

        private LogicalView mockView(LogicalSource viewOn) {
            var view = mock(LogicalView.class);
            when(view.getViewOn()).thenReturn(viewOn);
            lenient().when(view.getFields()).thenReturn(Set.of());
            return view;
        }

        private LogicalViewJoin mockJoin(LogicalView parentView) {
            var join = mock(LogicalViewJoin.class);
            when(join.getParentLogicalView()).thenReturn(parentView);
            lenient().when(join.getJoinConditions()).thenReturn(Set.of());
            return join;
        }

        private ExpressionField mockExpressionField(String name, String reference) {
            var field = mock(ExpressionField.class);
            lenient().when(field.getFieldName()).thenReturn(name);
            when(field.getExpressionMapExpressionSet()).thenReturn(Set.of(reference));
            return field;
        }
    }

    private static LogicalSourceResolver.ExpressionEvaluationFactory<Object> mockExprEvalFactory() {
        return rec -> expr -> Optional.empty();
    }

    private static LogicalSourceResolver.DatatypeMapperFactory<Object> mockDatatypeMapperFactory() {
        return rec -> value -> Optional.empty();
    }
}
