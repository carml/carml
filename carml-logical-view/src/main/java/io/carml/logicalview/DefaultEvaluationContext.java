package io.carml.logicalview;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;

@Builder
class DefaultEvaluationContext implements EvaluationContext {

    @Builder.Default
    private final Set<String> projectedFields = Set.of();

    @Builder.Default
    private final DedupStrategy dedupStrategy = DedupStrategy.none();

    private final Duration joinWindowDuration;

    private final Long joinWindowCount;

    private final Long limit;

    @Override
    public Set<String> getProjectedFields() {
        return projectedFields;
    }

    @Override
    public DedupStrategy getDedupStrategy() {
        return dedupStrategy;
    }

    @Override
    public Optional<Duration> getJoinWindowDuration() {
        return Optional.ofNullable(joinWindowDuration);
    }

    @Override
    public Optional<Long> getJoinWindowCount() {
        return Optional.ofNullable(joinWindowCount);
    }

    @Override
    public Optional<Long> getLimit() {
        return Optional.ofNullable(limit);
    }
}
