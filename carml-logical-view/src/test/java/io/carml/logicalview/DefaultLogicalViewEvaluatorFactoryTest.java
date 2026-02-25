package io.carml.logicalview;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import io.carml.model.LogicalView;
import org.junit.jupiter.api.Test;

class DefaultLogicalViewEvaluatorFactoryTest {

    private final DefaultLogicalViewEvaluatorFactory factory = new DefaultLogicalViewEvaluatorFactory();

    @Test
    void match_givenAnyLogicalView_thenAlwaysReturnsMatch() {
        var view = mock(LogicalView.class);

        var result = factory.match(view);

        assertThat(result.isPresent(), is(true));
    }

    @Test
    void match_givenAnyLogicalView_thenWeakScore() {
        var view = mock(LogicalView.class);

        var result = factory.match(view);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get().getMatchScore().getScore(), is(1));
    }

    @Test
    void match_givenAnyLogicalView_thenReturnsDefaultLogicalViewEvaluator() {
        var view = mock(LogicalView.class);

        var result = factory.match(view);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get().getLogicalViewEvaluator(), instanceOf(DefaultLogicalViewEvaluator.class));
    }
}
