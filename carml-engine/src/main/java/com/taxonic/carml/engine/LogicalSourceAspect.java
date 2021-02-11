package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.CreateContextEvaluate;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.GetStreamFromContext;
import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.MergeSuper;
import com.taxonic.carml.model.TriplesMap;
import org.eclipse.rdf4j.model.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.taxonic.carml.engine.RmlMapper.exception;

@SuppressWarnings("squid:S112")
class LogicalSourceAspect {

    private static final Logger LOG = LoggerFactory.getLogger(LogicalSourceAspect.class);

    private final Function<Object, String> sourceResolver;
    private final Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers;

    public LogicalSourceAspect(
        Function<Object, String> sourceResolver,
        Map<IRI, LogicalSourceResolver<?>> logicalSourceResolvers
    ) {
        this.sourceResolver = sourceResolver;
        this.logicalSourceResolvers = logicalSourceResolvers;
    }

    <T> LogicalSourceResolver<T> getLogicalSourceResolver(TriplesMap triplesMap) {

        LogicalSource logicalSource = triplesMap.getLogicalSource();

        if (logicalSource == null) {
            throw new RuntimeException(String.format("No LogicalSource found for TriplesMap%n%s", exception(triplesMap, triplesMap)));
        }

        return getLogicalSourceResolver(logicalSource);
    }

    private <T> LogicalSourceResolver<T> getLogicalSourceResolver(LogicalSource logicalSource) {

        IRI referenceFormulation = logicalSource.getReferenceFormulation();

        if (referenceFormulation == null) {
            throw new RuntimeException(
                String.format("No reference formulation found for LogicalSource %s", exception(logicalSource, logicalSource)));
        }

        if (!logicalSourceResolvers.containsKey(referenceFormulation)) {
            throw new RuntimeException(
                String.format("Unsupported reference formulation %s in LogicalSource %s", referenceFormulation,
                    exception(logicalSource, logicalSource)));
        }

        return (LogicalSourceResolver<T>) logicalSourceResolvers.get(referenceFormulation);
    }

    <T> Supplier<Stream<Item<T>>> createGetStream(TriplesMap triplesMap) {
        LogicalSourceResolver<T> logicalSourceResolver = getLogicalSourceResolver(triplesMap);
        return createGetStream(logicalSourceResolver, triplesMap.getLogicalSource());
    }

    private <T> Supplier<Stream<Item<T>>> createGetStream(LogicalSourceResolver<T> logicalSourceResolver, LogicalSource logicalSource) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Creating getStream from source {}" + (
                    logicalSource.getIterator() != null ? " with iterator expression: {}" : ""),
                logicalSource.getSource(), logicalSource.getIterator());
        }

        MergeSuper mergeSuper = logicalSource.getMergeSuper();
        if (mergeSuper != null) {

            Supplier<Stream<Item<Object>>> getSuperStream = createGetSuperStream(mergeSuper);

            UnaryOperator<EvaluateExpression> createEvaluateInContext =
                getCreateEvaluateInContext(logicalSourceResolver, mergeSuper);

            GetStreamFromContext<T> getStreamFromContext = logicalSourceResolver.createGetStreamFromContext(logicalSource.getIterator());

            return () -> {
                Stream<Item<Object>> superStream = getSuperStream.get();
                return superStream.flatMap(superEntry -> {
                    EvaluateExpression evaluateInContext = createEvaluateInContext.apply(superEntry.getEvaluate());
                    return getStreamFromContext.apply(evaluateInContext)
                        .map(i -> addContextEvaluation(i, evaluateInContext));
                });
            };
        }

        // standard
        return logicalSourceResolver.bindSource(logicalSource, sourceResolver);
    }

    // TODO duplicate code from ContextTriplesMapper
    private <T> Item<T> addContextEvaluation(Item<T> entry, EvaluateExpression evaluateInContext) {
        EvaluateExpression evaluateInEntry = entry.getEvaluate();
        EvaluateExpression evaluate = e -> {
            Optional<Object> result = evaluateInEntry.apply(e);
            if (result.isPresent()) {
                return result;
            }
            return evaluateInContext.apply(e);
        };
        return new Item<>(entry.getItem(), evaluate);
    }

    private Supplier<Stream<Item<Object>>> createGetSuperStream(MergeSuper mergeSuper) {
        LogicalSource logicalSource = mergeSuper.getLogicalSource();
        LogicalSourceResolver<Object> logicalSourceResolver = getLogicalSourceResolver(logicalSource);
        return createGetStream(logicalSourceResolver, logicalSource);
    }

    private <T> UnaryOperator<EvaluateExpression> getCreateEvaluateInContext(LogicalSourceResolver<T> logicalSourceResolver, MergeSuper mergeSuper) {
        Set<ContextEntry> including = mergeSuper.getIncluding();
        CreateContextEvaluate createContextEvaluate = logicalSourceResolver.getCreateContextEvaluate();
        return including.isEmpty()
            ? evaluateInSuperEntry -> evaluateInSuperEntry
            : evaluateInSuperEntry -> createContextEvaluate.apply(including, evaluateInSuperEntry);
    }
}
