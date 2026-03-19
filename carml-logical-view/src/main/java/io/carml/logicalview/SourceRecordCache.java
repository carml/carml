package io.carml.logicalview;

import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.model.ExpressionMap;
import io.carml.model.Field;
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.Source;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/**
 * Caches resolved source records keyed by {@link Source}. When the resolver produces records for
 * multiple LogicalSources sharing a Source, all records are stored under the same Source key. Views
 * filter cached records by their specific LogicalSource.
 *
 * <p>In addition to the records themselves, the cache stores the expression evaluation factory and
 * datatype mapper factory from the resolver that produced the records. This ensures that on cache
 * hit, the same resolver's factories are used — critical for resolvers like XPath where the parsed
 * nodes must be evaluated with the same Saxon Configuration that produced them.
 *
 * <p>Thread safety: the cache uses {@link ConcurrentHashMap} with {@link Mono#cache()} to ensure
 * at-most-once resolution per Source, even under concurrent subscription from multiple views.
 */
public final class SourceRecordCache {

    /**
     * Cached entry for a Source: the resolved records plus the resolver factories that produced them.
     */
    public record CachedRecords(
            List<LogicalSourceRecord<?>> records,
            LogicalSourceResolver.ExpressionEvaluationFactory<Object> expressionEvaluationFactory,
            LogicalSourceResolver.DatatypeMapperFactory<Object> datatypeMapperFactory) {}

    private final Map<Source, Mono<CachedRecords>> cache;

    private SourceRecordCache(Map<Source, Mono<CachedRecords>> cache) {
        this.cache = cache;
    }

    /**
     * Creates a real cache that stores and returns records.
     */
    public static SourceRecordCache create() {
        return new SourceRecordCache(new ConcurrentHashMap<>());
    }

    /**
     * Creates a no-op cache that always misses. Used to preserve lazy streaming when no shared
     * sources exist (single view, no joins).
     */
    public static SourceRecordCache noop() {
        return new SourceRecordCache(null);
    }

    /**
     * Returns a Mono that resolves to the cached records for the given source. On first call for a
     * given source, the resolver supplier is invoked and its result cached via {@link Mono#cache()}.
     * Subsequent calls for the same source share the same Mono, ensuring at-most-once resolution
     * even under concurrent subscription.
     *
     * @param source the source key
     * @param resolver supplier of the Mono that resolves records (called at most once per source)
     * @return a Mono of cached records
     */
    public Mono<CachedRecords> getOrResolve(Source source, Supplier<Mono<CachedRecords>> resolver) {
        if (cache == null) {
            return Mono.empty();
        }
        return cache.computeIfAbsent(source, k -> Mono.defer(resolver).cache());
    }

    /**
     * Returns {@code true} if this is a real cache (not a no-op).
     */
    public boolean isActive() {
        return cache != null;
    }

    /**
     * Pre-collects LogicalSource information from a set of views. For each view (and its join parent
     * views, recursively), identifies the root LogicalSource and collects all expressions used. This
     * information enables the engine to call a resolver once per Source with the full set of
     * LogicalSources and merged expressions.
     *
     * @param views the views to collect information from
     * @param logicalSourcesPerSource populated with the set of LogicalSources per Source
     * @param expressionsPerLogicalSource populated with the set of expressions per LogicalSource
     */
    public static void collectLogicalSourceInfo(
            Set<LogicalView> views,
            Map<Source, Set<LogicalSource>> logicalSourcesPerSource,
            Map<LogicalSource, Set<String>> expressionsPerLogicalSource) {
        var visited = new LinkedHashSet<LogicalView>();
        for (var view : views) {
            collectLogicalSourceInfoRecursive(view, logicalSourcesPerSource, expressionsPerLogicalSource, visited);
        }
    }

    private static void collectLogicalSourceInfoRecursive(
            LogicalView view,
            Map<Source, Set<LogicalSource>> logicalSourcesPerSource,
            Map<LogicalSource, Set<String>> expressionsPerLogicalSource,
            Set<LogicalView> visited) {
        if (!visited.add(view)) {
            return;
        }

        var viewOn = view.getViewOn();
        if (viewOn instanceof LogicalSource logicalSource) {
            logicalSourcesPerSource
                    .computeIfAbsent(logicalSource.getSource(), k -> new LinkedHashSet<>())
                    .add(logicalSource);
            var expressions = expressionsPerLogicalSource.computeIfAbsent(logicalSource, k -> new LinkedHashSet<>());
            collectFieldExpressions(view.getFields(), expressions);
            collectJoinChildExpressions(view, expressions);
        } else if (viewOn instanceof LogicalView parentView) {
            collectLogicalSourceInfoRecursive(
                    parentView, logicalSourcesPerSource, expressionsPerLogicalSource, visited);
        }

        // Recurse into join parent views
        collectJoinParentInfo(view.getLeftJoins(), logicalSourcesPerSource, expressionsPerLogicalSource, visited);
        collectJoinParentInfo(view.getInnerJoins(), logicalSourcesPerSource, expressionsPerLogicalSource, visited);
    }

    private static void collectJoinParentInfo(
            Set<LogicalViewJoin> joins,
            Map<Source, Set<LogicalSource>> logicalSourcesPerSource,
            Map<LogicalSource, Set<String>> expressionsPerLogicalSource,
            Set<LogicalView> visited) {
        if (joins == null) {
            return;
        }
        for (var join : joins) {
            collectLogicalSourceInfoRecursive(
                    join.getParentLogicalView(), logicalSourcesPerSource, expressionsPerLogicalSource, visited);
        }
    }

    private static void collectFieldExpressions(Set<Field> fields, Set<String> expressions) {
        if (fields == null) {
            return;
        }
        for (var field : fields) {
            if (field instanceof ExpressionMap expressionMap) {
                expressions.addAll(expressionMap.getExpressionMapExpressionSet());
            }
            if (field instanceof IterableField iterableField) {
                collectFieldExpressions(iterableField.getFields(), expressions);
            }
        }
    }

    private static void collectJoinChildExpressions(LogicalView view, Set<String> expressions) {
        var leftJoins = view.getLeftJoins();
        if (leftJoins != null) {
            for (var join : leftJoins) {
                join.getJoinConditions().stream()
                        .flatMap(c -> c.getChildMap().getExpressionMapExpressionSet().stream())
                        .forEach(expressions::add);
            }
        }
        var innerJoins = view.getInnerJoins();
        if (innerJoins != null) {
            for (var join : innerJoins) {
                join.getJoinConditions().stream()
                        .flatMap(c -> c.getChildMap().getExpressionMapExpressionSet().stream())
                        .forEach(expressions::add);
            }
        }
    }
}
