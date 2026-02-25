package io.carml.engine;

/**
 * No-op observer singleton. All methods are inherited defaults from {@link MappingExecutionObserver}
 * and are empty, making them JIT-inlinable for zero overhead when no observer is configured.
 */
public final class NoOpObserver implements MappingExecutionObserver {

    private static final NoOpObserver INSTANCE = new NoOpObserver();

    private NoOpObserver() {}

    public static NoOpObserver getInstance() {
        return INSTANCE;
    }
}
