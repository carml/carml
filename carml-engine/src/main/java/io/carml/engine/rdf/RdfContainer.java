package io.carml.engine.rdf;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.model.Target;
import io.carml.util.Models;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
@ToString
@Getter
public class RdfContainer<T extends Value> implements MappedValue<T>, MappingResult<Statement>, ModelBearing {

    private final IRI type;

    private final T container;

    private final Model model;

    @Singular
    private final Set<Target> targets;

    @Override
    public T getValue() {
        return container;
    }

    @Override
    public Set<Target> getTargets() {
        return targets;
    }

    @Override
    public Publisher<Statement> getResults() {
        return Flux.fromIterable(model);
    }

    public RdfContainer<T> withGraphs(Set<MappedValue<Resource>> mappedGraphs) {
        var graphTargets = mappedGraphs.stream() //
                .map(MappedValue::getTargets)
                .flatMap(Set::stream);

        var newTargets = Stream.concat(targets.stream(), graphTargets).collect(Collectors.toUnmodifiableSet());

        var graphs = mappedGraphs.stream() //
                .map(MappedValue::getValue)
                .collect(Collectors.toUnmodifiableSet());

        return toBuilder()
                .targets(newTargets)
                .model(Models.addGraphs(model, graphs))
                .build();
    }
}
