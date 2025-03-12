package io.carml.logicalsourceresolver;

import static java.util.Comparator.comparing;

import io.carml.logicalsourceresolver.LogicalSourceResolver.LogicalSourceResolverFactory;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor(staticName = "of")
@Getter
public class MatchedLogicalSourceResolverFactory {

    private MatchScore matchScore;

    private LogicalSourceResolverFactory<?> logicalSourceResolverFactory;

    public static Optional<LogicalSourceResolverFactory<?>> select(List<MatchedLogicalSourceResolverFactory> matches) {
        return matches.stream()
                .max(comparing(match -> match.getMatchScore().getScore()))
                .map(MatchedLogicalSourceResolverFactory::getLogicalSourceResolverFactory);
    }

    @AllArgsConstructor
    @Getter
    public static class MatchScore {

        private int score;

        public static Builder builder() {
            return new Builder();
        }

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Builder {

            private int score = 0;

            public Builder weakMatch() {
                score += 1;
                return this;
            }

            public Builder strongMatch() {
                score += 2;
                return this;
            }

            public MatchScore build() {
                return new MatchScore(score);
            }
        }
    }
}
