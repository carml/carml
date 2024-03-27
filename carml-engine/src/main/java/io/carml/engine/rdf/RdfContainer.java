package io.carml.engine.rdf;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.model.Target;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@SuperBuilder
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
}
