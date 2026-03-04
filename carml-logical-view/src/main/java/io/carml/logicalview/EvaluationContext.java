package io.carml.logicalview;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Provides contextual parameters for evaluating a {@link io.carml.model.LogicalView}. Controls
 * field projection, deduplication, join windowing, and result limiting.
 */
public interface EvaluationContext {

    String PROJECTED_FIELDS_NOT_NULL = "projectedFields must not be null";
    String LIMIT_MUST_BE_POSITIVE = "limit must be positive, but was: %s";

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
     * Returns whether the source-level expression evaluation should be retained in view iterations.
     * When {@code true}, the source evaluation is attached to each view iteration, allowing
     * expressions not captured as view fields (e.g., gather map references) to be evaluated
     * from the source data. This is intended for synthetic (implicit) views; explicit views
     * should return {@code false} to enforce strict field validation.
     *
     * @return {@code true} if source evaluations should be retained
     */
    default boolean retainSourceEvaluation() {
        return false;
    }

    /**
     * Returns a default batch evaluation context with all fields projected, no deduplication, no
     * join window, and no limit.
     *
     * @return a default evaluation context
     */
    static EvaluationContext defaults() {
        return DefaultEvaluationContext.builder().build();
    }

    /**
     * Creates an evaluation context that projects only the given fields. All other parameters
     * (dedup strategy, join window, limit) use their defaults.
     *
     * @param projectedFields the field names to project; an empty set means all fields
     * @return a new evaluation context with the specified projected fields
     */
    static EvaluationContext withProjectedFields(Set<String> projectedFields) {
        Objects.requireNonNull(projectedFields, PROJECTED_FIELDS_NOT_NULL);
        return DefaultEvaluationContext.builder()
                .projectedFields(Set.copyOf(projectedFields))
                .build();
    }

    /**
     * Creates an evaluation context that projects the given fields and optionally limits the number
     * of iterations produced.
     *
     * @param projectedFields the field names to project; an empty set means all fields
     * @param limit the maximum number of iterations to produce, or {@code null} for no limit
     * @return a new evaluation context with the specified projected fields and limit
     * @throws IllegalArgumentException if limit is non-null and not positive
     */
    static EvaluationContext withProjectedFieldsAndLimit(Set<String> projectedFields, Long limit) {
        Objects.requireNonNull(projectedFields, PROJECTED_FIELDS_NOT_NULL);
        if (limit != null && limit <= 0) {
            throw new IllegalArgumentException(LIMIT_MUST_BE_POSITIVE.formatted(limit));
        }
        return DefaultEvaluationContext.builder()
                .projectedFields(Set.copyOf(projectedFields))
                .limit(limit)
                .build();
    }

    /**
     * Creates an evaluation context for an implicit (synthetic) view. All fields are projected,
     * source evaluations are retained, and an optional limit is applied. Retaining source
     * evaluations allows expressions not captured as view fields (e.g., gather map references)
     * to be evaluated from the source data.
     *
     * @param limit the maximum number of iterations to produce, or {@code null} for no limit
     * @return a new evaluation context for an implicit view
     * @throws IllegalArgumentException if limit is non-null and not positive
     */
    static EvaluationContext forImplicitView(Long limit) {
        if (limit != null && limit <= 0) {
            throw new IllegalArgumentException(LIMIT_MUST_BE_POSITIVE.formatted(limit));
        }
        return DefaultEvaluationContext.builder()
                .retainSourceEvaluation(true)
                .limit(limit)
                .build();
    }

    /**
     * Creates an evaluation context with the given projected fields, dedup strategy, and optional
     * limit.
     *
     * @param projectedFields the field names to project; an empty set means all fields
     * @param dedupStrategy the deduplication strategy to apply
     * @param limit the maximum number of iterations to produce, or {@code null} for no limit
     * @return a new evaluation context
     * @throws IllegalArgumentException if limit is non-null and not positive
     */
    static EvaluationContext of(Set<String> projectedFields, DedupStrategy dedupStrategy, Long limit) {
        Objects.requireNonNull(projectedFields, PROJECTED_FIELDS_NOT_NULL);
        Objects.requireNonNull(dedupStrategy, "dedupStrategy must not be null");
        if (limit != null && limit <= 0) {
            throw new IllegalArgumentException(LIMIT_MUST_BE_POSITIVE.formatted(limit));
        }
        return DefaultEvaluationContext.builder()
                .projectedFields(Set.copyOf(projectedFields))
                .dedupStrategy(dedupStrategy)
                .limit(limit)
                .build();
    }
}
