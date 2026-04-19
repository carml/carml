package io.carml.engine.target;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.carml.engine.CheckpointInfo;
import io.carml.engine.MappingExecutionResult;
import io.carml.engine.ResolvedMapping;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import io.carml.model.TermMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.rdf4j.model.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TargetRouterTest {

    @Mock
    ResolvedMapping mapping;

    @Mock
    ViewIteration viewIteration;

    @Mock
    Statement statement;

    @Mock
    CheckpointInfo checkpoint;

    @Mock
    MappingExecutionResult result;

    @Test
    void onStatementGenerated_withLogicalTarget_routesToTargetWriter() {
        // Given
        var logicalTarget = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);
        var termMap = mock(TermMap.class);
        when(termMap.getLogicalTargets()).thenReturn(Set.of(logicalTarget));

        try (var router = new TargetRouter(Map.of(logicalTarget, targetWriter), defaultWriter)) {
            // When
            router.onStatementGenerated(mapping, viewIteration, statement, termMap);

            // Then
            verify(targetWriter).write(statement);
            verifyNoInteractions(defaultWriter);
        }
    }

    @Test
    void onStatementGenerated_withoutLogicalTarget_routesToDefaultWriter() {
        // Given
        var defaultWriter = mock(TargetWriter.class);
        var termMap = mock(TermMap.class);
        when(termMap.getLogicalTargets()).thenReturn(Set.of());

        try (var router = new TargetRouter(Map.of(), defaultWriter)) {
            // When
            router.onStatementGenerated(mapping, viewIteration, statement, termMap);

            // Then
            verify(defaultWriter).write(statement);
        }
    }

    @Test
    void onStatementGenerated_withoutLogicalTargetOrDefault_silentlyDrops() {
        // Given
        var termMap = mock(TermMap.class);
        when(termMap.getLogicalTargets()).thenReturn(Set.of());

        try (var router = new TargetRouter(Map.of(), null)) {
            // When
            router.onStatementGenerated(mapping, viewIteration, statement, termMap);

            // Then - no writers to interact with, statement is silently dropped
            assertThat(router.hasDefaultWriter(), is(false));
        }
    }

    @Test
    void onStatementGenerated_multipleTargets_routesToAll() {
        // Given
        var target1 = mock(LogicalTarget.class);
        var target2 = mock(LogicalTarget.class);
        var writer1 = mock(TargetWriter.class);
        var writer2 = mock(TargetWriter.class);
        var termMap = mock(TermMap.class);
        when(termMap.getLogicalTargets()).thenReturn(Set.of(target1, target2));

        try (var router = new TargetRouter(Map.of(target1, writer1, target2, writer2), null)) {
            // When
            router.onStatementGenerated(mapping, viewIteration, statement, termMap);

            // Then
            verify(writer1).write(statement);
            verify(writer2).write(statement);
        }
    }

    @Test
    void onStatementGenerated_missingWriterForDeclaredTarget_throwsIllegalState() {
        // Given - termMap declares a target that has no registered writer
        var declaredTarget = mock(LogicalTarget.class);
        var unrelatedTarget = mock(LogicalTarget.class);
        var unrelatedWriter = mock(TargetWriter.class);
        var termMap = mock(TermMap.class);
        when(termMap.getLogicalTargets()).thenReturn(Set.of(declaredTarget));

        try (var router = new TargetRouter(Map.of(unrelatedTarget, unrelatedWriter), null)) {
            // When / Then - missing writer is a configuration bug that must surface as error, not
            // be silently dropped. This avoids producing incomplete RDF output for a misconfigured
            // mapping.
            assertThrows(
                    IllegalStateException.class,
                    () -> router.onStatementGenerated(mapping, viewIteration, statement, termMap));
            verifyNoInteractions(unrelatedWriter);
        }
    }

    @Test
    void open_opensAllWriters() {
        // Given
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), defaultWriter)) {
            // When
            router.open();

            // Then
            verify(targetWriter).open();
            verify(defaultWriter).open();
        }
    }

    @Test
    void onMappingStart_autoOpensWriters() {
        // Given
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), null)) {
            // When
            router.onMappingStart(mapping);

            // Then
            verify(targetWriter).open();
        }
    }

    @Test
    void open_calledTwice_opensOnlyOnce() {
        // Given
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), null)) {
            // When
            router.open();
            router.open();

            // Then
            verify(targetWriter).open();
        }
    }

    @Test
    @SuppressWarnings(
            "resource") // ExecutorService.close() is Java 19+; project compiles to 17 — shutdownNow in finally
    void open_concurrentInvocations_opensExactlyOnce() throws InterruptedException {
        // Given - simulate multiple reactive threads racing on onMappingStart.
        // Before the synchronized lifecycle lock was introduced, two threads could both pass the
        // "already opened?" check before either set the flag and both call writer.open(), which
        // now (Task 7.7) throws IllegalStateException on double-open. This test guards the race.
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);

        int threads = 32;
        var executor = Executors.newFixedThreadPool(threads);
        try (var router = new TargetRouter(Map.of(target, targetWriter), null)) {
            var startGate = new CountDownLatch(1);
            var finishGate = new CountDownLatch(threads);
            var failures = new AtomicInteger();

            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        startGate.await();
                        router.open();
                    } catch (RuntimeException | InterruptedException ex) {
                        failures.incrementAndGet();
                    } finally {
                        finishGate.countDown();
                    }
                });
            }

            startGate.countDown();
            assertThat(finishGate.await(5, TimeUnit.SECONDS), is(true));

            // Then
            assertThat(failures.get(), is(0));
            verify(targetWriter, times(1)).open();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void open_defaultWriterThrows_closesAlreadyOpenedTargetWritersAndRethrows() {
        // Given - registered target writer opens OK, but the default writer (which is opened last
        // by contract) fails. Router must close the already-opened target writer so no resources
        // leak, and must leave itself in a non-open state. This uses the default writer as the
        // failure point because the ordering of the registered map's iteration is unspecified,
        // whereas default-writer-last is a stable contract.
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);
        var boom = new RuntimeException("disk full");
        doThrow(boom).when(defaultWriter).open();

        try (var router = new TargetRouter(Map.of(target, targetWriter), defaultWriter)) {
            // When
            var thrown = assertThrows(RuntimeException.class, router::open);

            // Then - the failing writer's exception is rethrown as-is
            assertThat(thrown, sameInstance(boom));

            // The target writer was opened; rollback must have closed it.
            verify(targetWriter).open();
            verify(targetWriter).close();

            // The default writer never successfully opened; its close() must not be invoked
            // during rollback (only the writers that successfully opened are rolled back).
            verify(defaultWriter).open();
            verify(defaultWriter, never()).close();

            // No suppressed exceptions since rollback succeeded cleanly
            assertThat(thrown.getSuppressed(), is(emptyArray()));
        }
    }

    @Test
    void open_defaultWriterThrows_suppressesSecondaryCloseFailures() {
        // Given - target writer opens OK, default writer fails on open, and the already-opened
        // target writer's close() itself throws during rollback. The secondary close failure must
        // be attached as suppressed on the primary open failure (which remains the top-level
        // exception) so diagnostic context is preserved.
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);
        var openFailure = new RuntimeException("permission denied");
        var closeFailure = new RuntimeException("close also failed");

        doThrow(closeFailure).when(targetWriter).close();
        doThrow(openFailure).when(defaultWriter).open();

        try (var router = new TargetRouter(Map.of(target, targetWriter), defaultWriter)) {
            // When
            var thrown = assertThrows(RuntimeException.class, router::open);

            // Then
            assertThat(thrown, sameInstance(openFailure));
            assertThat(thrown.getSuppressed(), hasItemInArray(closeFailure));
        }
    }

    @Test
    void onCheckpoint_flushesAllWriters() {
        // Given
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), defaultWriter)) {
            // When
            router.onCheckpoint(mapping, checkpoint);

            // Then
            verify(targetWriter).flush();
            verify(defaultWriter).flush();
        }
    }

    @Test
    // explicitly verifying pre-close state; router is closed manually below
    void onMappingComplete_doesNotCloseWriters() {
        // Given - onMappingComplete is a no-op at the router level. It does not close writers
        // because onMappingComplete can fire without a matching onMappingStart (empty source
        // fluxes, filtered views, upstream errors), so a counter-driven auto-close would
        // prematurely close writers while other concurrent mappings are still writing. Close
        // is driven exclusively by the caller via try-with-resources or explicit close().
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);

        var router = new TargetRouter(Map.of(target, targetWriter), defaultWriter);
        router.onMappingStart(mapping);

        // When
        router.onMappingComplete(mapping, result);

        // Then - writers are NOT closed by onMappingComplete
        verify(targetWriter, never()).close();
        verify(defaultWriter, never()).close();

        // Cleanup: caller closes the router explicitly.
        router.close();
        verify(targetWriter, times(1)).close();
        verify(defaultWriter, times(1)).close();
    }

    @Test
    void onMappingComplete_multipleMappingStarts_doesNotClose() {
        // Given - even with multiple onMappingStart events, onMappingComplete must not close
        // writers. The router no longer tracks a per-mapping counter: close is caller-driven.
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), null)) {
            router.onMappingStart(mapping);
            router.onMappingStart(mapping);

            // When - a mapping completes
            router.onMappingComplete(mapping, result);

            // Then - writers not closed yet (close happens only via try-with-resources exit)
            verify(targetWriter, never()).close();
        }

        // After try-with-resources exit, caller close closes once.
        verify(targetWriter, times(1)).close();
    }

    @Test
    void close_closesAllWriters() {
        // Given
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);

        // When - close() is driven by try-with-resources exit. Verification happens after the
        // block so there is no explicit close() call on the auto-closeable resource.
        try (var router = new TargetRouter(Map.of(target, targetWriter), defaultWriter)) {
            // no-op body; close is invoked via try-with-resources
            assertThat(router.hasDefaultWriter(), is(true));
        }

        // Then
        verify(targetWriter).close();
        verify(defaultWriter).close();
    }

    @Test
    // explicit double-close on the router is the behavior under test
    void close_calledTwice_onlyClosesOnce() {
        // Given - verifies the router's close() is idempotent at its own layer (the individual
        // TargetWriter.close() contract is already idempotent per Task 7.7, but the router should
        // not trigger duplicate close() calls via its own state machine).
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);

        var router = new TargetRouter(Map.of(target, targetWriter), null);

        // When
        router.close();
        router.close();

        // Then
        verify(targetWriter, times(1)).close();
    }

    @Test
    // "try" suppresses the warning for the explicit close() inside try-with-resources; the second
    // close from the try-with-resources exit is a no-op by the router's idempotent close contract.
    @SuppressWarnings("try")
    void open_afterClose_throwsIllegalState() {
        // Given - router is single-use. Opening a freshly-closed router must fail fast rather
        // than silently no-op, because the underlying writers are already closed and any
        // subsequent write attempt would throw.
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), null)) {
            router.close();

            // When / Then
            assertThrows(IllegalStateException.class, router::open);
        } // second close from try-with-resources is a no-op
    }

    @Test
    // "try" suppresses the warning for the explicit close() inside try-with-resources; the second
    // close from the try-with-resources exit is a no-op by the router's idempotent close contract.
    @SuppressWarnings("try")
    void onMappingStart_afterClose_throwsIllegalState() {
        // Given - same contract as open-after-close, surfaced via the observer entry point.
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), null)) {
            router.close();

            // When / Then
            assertThrows(IllegalStateException.class, () -> router.onMappingStart(mapping));
        } // second close from try-with-resources is a no-op
    }

    @Test
    void onMappingCompleteAndExplicitClose_closesWritersExactlyOnce() {
        // Given - onMappingComplete is a no-op at the router level, so writers are only closed
        // by the caller's explicit close() (here via try-with-resources exit). Verifies the
        // router closes writers exactly once over the combined sequence.
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), null)) {
            router.onMappingStart(mapping);
            router.onMappingComplete(mapping, result);
            // When - try-with-resources exit triggers close()
        }

        // Then
        verify(targetWriter, times(1)).close();
    }

    @Test
    void open_failed_thenExplicitClose_closesWritersOnlyOnce() {
        // Given - writer A opens successfully, writer B throws on open. Rollback must close
        // writer A once; the subsequent explicit close() (via try-with-resources) must be a
        // no-op because open() marks the router terminally closed on failure. This guards
        // against calling close() on a writer whose open() threw (where close behavior is
        // undefined per TargetWriter contract) and against closing writer A twice at the
        // router layer.
        var targetA = mock(LogicalTarget.class);
        var writerA = mock(TargetWriter.class);
        var defaultWriterB = mock(TargetWriter.class);
        var openFailure = new RuntimeException("writer B open failed");
        doThrow(openFailure).when(defaultWriterB).open();

        try (var router = new TargetRouter(Map.of(targetA, writerA), defaultWriterB)) {
            assertThrows(RuntimeException.class, router::open);
            // When - try-with-resources exit calls close() on the already-terminally-closed
            // router. This must be a no-op and must NOT re-close writer A or attempt to close
            // writer B.
        }

        // Then - writer A closed exactly once (by rollback); writer B never closed.
        verify(writerA, times(1)).close();
        verify(defaultWriterB, never()).close();
    }

    @Test
    void lifecycle_openWriteFlushClose_inOrder() {
        // Given - sanity check that the standard lifecycle invokes writer methods in the
        // documented order. Close happens only at try-with-resources exit (onMappingComplete
        // is a no-op at the router level), so close verification runs after the block.
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var termMap = mock(TermMap.class);
        when(termMap.getLogicalTargets()).thenReturn(Set.of(target));

        try (var router = new TargetRouter(Map.of(target, targetWriter), null)) {
            router.onMappingStart(mapping);
            router.onStatementGenerated(mapping, viewIteration, statement, termMap);
            router.onCheckpoint(mapping, checkpoint);
            router.onMappingComplete(mapping, result);
        }

        // Then
        var inOrder = inOrder(targetWriter);
        inOrder.verify(targetWriter).open();
        inOrder.verify(targetWriter).write(statement);
        inOrder.verify(targetWriter).flush();
        inOrder.verify(targetWriter).close();
    }

    @Test
    void getLogicalTargets_returnsRegisteredTargets() {
        // Given
        var target1 = mock(LogicalTarget.class);
        var target2 = mock(LogicalTarget.class);

        try (var router =
                new TargetRouter(Map.of(target1, mock(TargetWriter.class), target2, mock(TargetWriter.class)), null)) {
            // Then
            assertThat(router.getLogicalTargets(), is(Set.of(target1, target2)));
        }
    }

    @Test
    void hasDefaultWriter_withDefault_returnsTrue() {
        try (var router = new TargetRouter(Map.of(), mock(TargetWriter.class))) {
            assertThat(router.hasDefaultWriter(), is(true));
        }
    }

    @Test
    void hasDefaultWriter_withoutDefault_returnsFalse() {
        try (var router = new TargetRouter(Map.of(), null)) {
            assertThat(router.hasDefaultWriter(), is(false));
        }
    }

    @Test
    void close_neverOpened_closesRegisteredWriters() {
        // Given - router is never opened (no open() or onMappingStart call).
        // Closing an unused router must still close its registered writers to release resources
        // that the caller may have already allocated (e.g. an eagerly-constructed TargetWriter)
        var logicalTarget = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);
        var router = new TargetRouter(Map.of(logicalTarget, targetWriter), defaultWriter);

        // When - explicit close on a never-opened router (not try-with-resources because the
        // close IS the action under test; try-with-resources would require a non-empty body)
        router.close();

        // Then
        verify(targetWriter, times(1)).close();
        verify(defaultWriter, times(1)).close();
        verify(targetWriter, never()).open();
        verify(defaultWriter, never()).open();
    }

    @Test
    // explicit close is the behavior under test
    void onCheckpoint_afterClose_doesNotFlush() {
        // Given - the closed flag early-returns onCheckpoint to avoid flushing writers that have
        // already been closed (which has unspecified behavior per the TargetWriter contract)
        var logicalTarget = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);

        var router = new TargetRouter(Map.of(logicalTarget, targetWriter), defaultWriter);
        router.open();
        router.close();

        // When
        router.onCheckpoint(mapping, checkpoint);

        // Then - no flush was invoked after close
        verify(targetWriter, never()).flush();
        verify(defaultWriter, never()).flush();
    }

    @Test
    // explicit close is the behavior under test
    void onStatementGenerated_afterClose_propagatesWriterException() {
        // Given - the router does not guard onStatementGenerated against post-close state. Writes
        // are delegated to the underlying TargetWriter, whose write() throws IllegalStateException
        // when invoked after close per the Task 7.7 contract. Pin this behavior so it does not
        // regress into silent drops
        var logicalTarget = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var termMap = mock(TermMap.class);
        when(termMap.getLogicalTargets()).thenReturn(Set.of(logicalTarget));
        doThrow(new IllegalStateException("writer already closed"))
                .when(targetWriter)
                .write(statement);

        var router = new TargetRouter(Map.of(logicalTarget, targetWriter), null);
        router.open();
        router.close();

        // When / Then
        var exception = assertThrows(
                IllegalStateException.class,
                () -> router.onStatementGenerated(mapping, viewIteration, statement, termMap));
        assertThat(exception.getMessage(), is("writer already closed"));
    }
}
