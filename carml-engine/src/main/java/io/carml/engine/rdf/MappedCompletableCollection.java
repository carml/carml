package io.carml.engine.rdf;

import io.carml.engine.Completable;
import io.carml.engine.MappingResult;
import io.carml.model.Target;
import java.util.Set;
import lombok.Builder;
import lombok.Singular;
import org.eclipse.rdf4j.model.Statement;
import org.reactivestreams.Publisher;

@Builder
public class MappedCompletableCollection implements MappingResult<Completable<Statement>> {

    private Statement statement;

    @Singular
    private Set<Target> targets;

    @Override
    public Set<Target> getTargets() {
        return null;
    }

    @Override
    public Publisher<Completable<Statement>> getResults() {
        return null;
    }
}
