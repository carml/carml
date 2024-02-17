package io.carml.logicalsourceresolver;

import static java.util.Comparator.comparing;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor(staticName = "of")
@Getter
public class MatchedLogicalSourceResolverSupplier {

  private MatchScore matchScore;

  private Supplier<LogicalSourceResolver<?>> logicalSourceResolverSupplier;

  public static Optional<Supplier<LogicalSourceResolver<?>>> select(
      List<MatchedLogicalSourceResolverSupplier> matches) {
    return matches.stream()
        .max(comparing(match -> match.getMatchScore()
            .getScore()))
        .map(MatchedLogicalSourceResolverSupplier::getLogicalSourceResolverSupplier);
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
