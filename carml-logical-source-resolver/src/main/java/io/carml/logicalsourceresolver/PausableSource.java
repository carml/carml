package io.carml.logicalsourceresolver;

/**
 * Abstraction over a push-based data source that can be paused and resumed. Used by
 * {@link PausableFluxBridge} to bridge push-based parsers (SAX/XMLDog, JSurfer) to reactive
 * {@code Flux} streams with proper backpressure support.
 *
 * <p>Implementations wrap parser-specific lifecycle (e.g. StAX reader pause/resume, byte-reading
 * loop park/unpark) behind a uniform interface.
 */
public interface PausableSource {

    /**
     * Starts the source. May block the calling thread (e.g. JSONPath read loop) or return
     * immediately (e.g. XPath sniff).
     *
     * @throws LogicalSourceResolverException if the source fails to start
     */
    void start();

    /** Pauses the source. Called from the emitting thread when demand is exhausted. */
    void pause();

    /**
     * Resumes the source. Called from the {@code onRequest} callback thread when demand recovers.
     *
     * @throws LogicalSourceResolverException if the source fails to resume
     */
    void resume();

    /** Returns {@code true} if the source is currently paused. */
    boolean isPaused();

    /** Returns {@code true} if the source has completed producing items. */
    boolean isCompleted();
}
