package io.carml.engine.rdf.cc;

import io.carml.engine.MappedValue;
import java.util.Set;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Getter
public class RdfList<T extends Value> extends AbstractRdfMappedStatementCollection<T> {

    private final T head;

    @Override
    public T getValue() {
        return head;
    }

    @Override
    protected Stream<Statement> buildOwnStatements() {
        return RdfCollectionsAndContainers.toRdfListStatements(
                getElements(), (Resource) head, SimpleValueFactory.getInstance());
    }

    public RdfList<T> withGraphs(Set<MappedValue<Resource>> mappedGraphs) {
        var merged = mergeGraphs(mappedGraphs);
        return toBuilder()
                .logicalTargets(merged.logicalTargets())
                .graphs(merged.graphs())
                .build();
    }
}
