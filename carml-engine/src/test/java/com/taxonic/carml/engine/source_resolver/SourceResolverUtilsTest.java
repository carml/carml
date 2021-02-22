package com.taxonic.carml.engine.source_resolver;

import com.taxonic.carml.model.impl.CarmlFileSource;
import com.taxonic.carml.model.impl.CarmlStream;
import org.junit.Test;

import java.util.Optional;

import static com.taxonic.carml.engine.source_resolver.SourceResolverUtils.unpackFileSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SourceResolverUtilsTest {

    @Test
    public void testUnpackStringSource() {
        assertThat(unpackFileSource("a source"), is(Optional.of("a source")));
    }

    @Test
    public void testUnpackFileSource() {
        CarmlFileSource source = new CarmlFileSource();
        source.setUrl("some url");
        assertThat(unpackFileSource(source), is(Optional.of("some url")));
    }

    @Test
    public void testUnpackOtherYieldsEmpty() {
        assertThat(unpackFileSource(null), is(Optional.empty()));
        assertThat(unpackFileSource(String.class), is(Optional.empty()));
        assertThat(unpackFileSource(new CarmlStream()), is(Optional.empty()));
    }
}