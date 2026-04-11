package io.carml.engine.target;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    void onMappingComplete_lastMapping_closesAllWriters() {
        // Given
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), defaultWriter)) {
            router.onMappingStart(mapping);

            // When
            router.onMappingComplete(mapping, result);

            // Then
            verify(targetWriter).close();
            verify(defaultWriter).close();
        }
    }

    @Test
    void onMappingComplete_notLastMapping_doesNotClose() {
        // Given
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);

        try (var router = new TargetRouter(Map.of(target, targetWriter), null)) {
            router.onMappingStart(mapping);
            router.onMappingStart(mapping);

            // When - first mapping completes
            router.onMappingComplete(mapping, result);

            // Then - writers not closed yet
            verify(targetWriter, never()).close();
        }
    }

    @Test
    void close_closesAllWriters() {
        // Given
        var target = mock(LogicalTarget.class);
        var targetWriter = mock(TargetWriter.class);
        var defaultWriter = mock(TargetWriter.class);

        var router = new TargetRouter(Map.of(target, targetWriter), defaultWriter);

        // When
        router.close();

        // Then
        verify(targetWriter).close();
        verify(defaultWriter).close();
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
}
