package io.carml.csv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class CsvDialectConfigTest {

    @Test
    void defaults_hasAllEmptyOptionals() {
        var defaults = CsvDialectConfig.DEFAULTS;

        assertThat(defaults.delimiter(), is(Optional.empty()));
        assertThat(defaults.quoteChar(), is(Optional.empty()));
        assertThat(defaults.commentPrefix(), is(Optional.empty()));
        assertThat(defaults.encoding(), is(Optional.empty()));
        assertThat(defaults.skipRows(), is(0));
        assertThat(defaults.useDoubleQuoteEscaping(), is(true));
        assertThat(defaults.trim(), is(false));
    }
}
