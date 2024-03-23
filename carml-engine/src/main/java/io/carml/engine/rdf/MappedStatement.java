package io.carml.engine.rdf;

import io.carml.engine.MappingResult;
import io.carml.model.Target;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.ToString;
import org.eclipse.rdf4j.model.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

@Builder
@EqualsAndHashCode
@ToString
public class MappedStatement implements MappingResult<Statement> {

    private Statement statement;

    @Singular
    private Set<Target> targets;

    @Override
    public Set<Target> getTargets() {
        return targets;
    }

    @Override
    public Publisher<Statement> getResults() {
        return Mono.just(statement);
    }
}
