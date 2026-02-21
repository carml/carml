package io.carml.logicalview;

import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.LogicalSource;
import reactor.core.publisher.Mono;

/**
 * Inspects a resolved data source and discovers its schema (available fields, types, structure).
 * This is primarily used by ModelDesk's UI, where users build {@link io.carml.model.LogicalView}
 * definitions step-by-step and need to see what fields are available in a source.
 *
 * <p>Format-specific implementations (JSON, XML, CSV, SQL) are provided by separate modules and
 * discovered via {@link SourceIntrospectorFactory} and {@link java.util.ServiceLoader}.
 */
public interface SourceIntrospector {

    /**
     * Introspects the given logical source and its resolved data, returning a schema describing the
     * available fields.
     *
     * @param logicalSource the logical source definition (contains reference formulation, iterator,
     *     etc.)
     * @param resolvedSource the resolved source data to introspect
     * @return a mono emitting the discovered source schema
     */
    Mono<SourceSchema> introspect(LogicalSource logicalSource, ResolvedSource<?> resolvedSource);
}
