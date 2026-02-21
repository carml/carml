package io.carml.logicalview;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

/**
 * Provides contextual parameters for evaluating a {@link io.carml.model.LogicalView}. Controls
 * field projection, deduplication, join windowing, and result limiting.
 */
public interface EvaluationContext {

    /**
     * Returns the set of fields to project from the view. An empty set indicates all fields should
     * be projected.
     *
     * @return the set of projected field names
     */
    Set<String> getProjectedFields();

    /**
     * Returns the deduplication strategy to apply to the evaluation result.
     *
     * @return the dedup strategy
     */
    DedupStrategy getDedupStrategy();

    /**
     * Returns the optional time-based join window duration for streaming joins.
     *
     * @return an optional window duration
     */
    Optional<Duration> getJoinWindowDuration();

    /**
     * Returns the optional count-based join window size for streaming joins.
     *
     * @return an optional window count
     */
    Optional<Long> getJoinWindowCount();

    /**
     * Returns the optional limit on the number of iterations to produce.
     *
     * @return an optional limit
     */
    Optional<Long> getLimit();

    /**
     * Returns a default batch evaluation context with all fields projected, exact deduplication, no
     * join window, and no limit.
     *
     * @return a default evaluation context
     */
    static EvaluationContext defaults() {
        return new EvaluationContext() {
            @Override
            public Set<String> getProjectedFields() {
                return Set.of();
            }

            @Override
            public DedupStrategy getDedupStrategy() {
                return DedupStrategy.exact();
            }

            @Override
            public Optional<Duration> getJoinWindowDuration() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getJoinWindowCount() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getLimit() {
                return Optional.empty();
            }
        };
    }
}
