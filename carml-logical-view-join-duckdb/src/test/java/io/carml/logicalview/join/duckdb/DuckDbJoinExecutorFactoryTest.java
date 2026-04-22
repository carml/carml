package io.carml.logicalview.join.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DuckDbJoinExecutorFactoryTest {

    @Test
    void create_returnsFreshInstancePerCall(@TempDir Path spillDir) {
        var factory = new DuckDbJoinExecutorFactory(1000, true, spillDir);
        try (var first = factory.create();
                var second = factory.create()) {
            assertThat(first, is(not(sameInstance(second))));
        }
    }

    @Test
    void constructor_negativeThreshold_throws(@TempDir Path spillDir) {
        var ex = assertThrows(IllegalArgumentException.class, () -> new DuckDbJoinExecutorFactory(-1, true, spillDir));
        assertThat(ex.getMessage().contains("spillThreshold"), is(true));
    }

    @Test
    void constructor_fileBackedNullSpillDir_throws() {
        var ex = assertThrows(IllegalArgumentException.class, () -> new DuckDbJoinExecutorFactory(100, true, null));
        assertThat(ex.getMessage().contains("spillDir"), is(true));
    }

    @Test
    void constructor_inMemoryMode_acceptsNullSpillDir() {
        var factory = new DuckDbJoinExecutorFactory(100, false, null);
        try (var executor = factory.create()) {
            assertThat(executor, is(notNullValue()));
        }
    }
}
