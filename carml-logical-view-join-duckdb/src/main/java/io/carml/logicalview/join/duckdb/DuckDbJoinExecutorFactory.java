package io.carml.logicalview.join.duckdb;

import io.carml.logicalview.JoinExecutor;
import io.carml.logicalview.JoinExecutorFactory;
import java.nio.file.Path;

/**
 * {@link JoinExecutorFactory} for {@link DuckDbJoinExecutor}. Configured with a spill threshold
 * (parent-row count above which the executor switches from HashMap probe to DuckDB SQL HASH JOIN),
 * a file-backed flag (when true, the executor opens an on-disk DuckDB database under {@code
 * spillDir} so the buffer manager can spill intermediates; when false, opens an in-memory DuckDB
 * connection), and the directory used for spill files.
 *
 * <p>Constructed explicitly by the application — there is no {@code @AutoService} discovery, since
 * the spillable executor is opt-in via the CLI.
 */
public final class DuckDbJoinExecutorFactory implements JoinExecutorFactory {

    private final int spillThreshold;

    private final boolean fileBacked;

    private final Path spillDir;

    public DuckDbJoinExecutorFactory(int spillThreshold, boolean fileBacked, Path spillDir) {
        if (spillThreshold < 0) {
            throw new IllegalArgumentException("spillThreshold must be >= 0, was %d".formatted(spillThreshold));
        }
        if (fileBacked && spillDir == null) {
            throw new IllegalArgumentException("spillDir must not be null when fileBacked=true");
        }
        this.spillThreshold = spillThreshold;
        this.fileBacked = fileBacked;
        this.spillDir = spillDir;
    }

    @Override
    public JoinExecutor create() {
        return new DuckDbJoinExecutor(spillThreshold, fileBacked, spillDir);
    }
}
