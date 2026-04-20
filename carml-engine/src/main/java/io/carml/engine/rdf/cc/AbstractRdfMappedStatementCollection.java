package io.carml.engine.rdf.cc;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.rdf.ModelBearing;
import io.carml.model.LogicalTarget;
import io.carml.util.Models;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Shared base for {@link MappedValue}/{@link MappingResult} implementations that carry an in-memory
 * {@link Model} and forward its statements as the mapping result (e.g. {@link RdfList},
 * {@link RdfContainer}). Holds the common {@code model} and {@code logicalTargets} state plus the
 * graph-merge logic used by subclass {@code withGraphs(...)} methods.
 */
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
@ToString
@Getter
public abstract class AbstractRdfMappedStatementCollection<T extends Value>
        implements MappedValue<T>, MappingResult<Statement>, ModelBearing {

    private final Model model;

    @Singular
    private final Set<LogicalTarget> logicalTargets;

    @Override
    public Set<LogicalTarget> getLogicalTargets() {
        return logicalTargets;
    }

    @Override
    public Publisher<Statement> getResults() {
        return Flux.fromIterable(model);
    }

    /**
     * Computes the {@code (logicalTargets, model)} pair resulting from merging the given mapped
     * graphs into this collection. Subclasses use this to implement their {@code withGraphs(...)}
     * so the common logic is shared while the subclass-specific builder return type is preserved.
     *
     * @param mappedGraphs the mapped graph resources to merge in
     * @return merged logical targets and model
     */
    protected GraphMerge mergeGraphs(Set<MappedValue<Resource>> mappedGraphs) {
        var graphTargets = mappedGraphs.stream() //
                .map(MappedValue::getLogicalTargets)
                .flatMap(Set::stream);

        var newTargets = Stream.concat(logicalTargets.stream(), graphTargets).collect(Collectors.toUnmodifiableSet());

        var graphs = mappedGraphs.stream() //
                .map(MappedValue::getValue)
                .collect(Collectors.toUnmodifiableSet());

        return new GraphMerge(newTargets, Models.addGraphs(model, graphs));
    }

    protected record GraphMerge(Set<LogicalTarget> logicalTargets, Model model) {}
}
