package io.carml.engine.rdf;

import io.carml.engine.Completable;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Statement;

public class CompletableRdfCollection implements Completable<Statement> {

    @Override
    public Stream<Statement> complete() {
        return null;
    }
}
