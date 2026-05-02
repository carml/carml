package io.carml.engine.rdf.cc;

import io.carml.engine.MappedValue;
import io.carml.model.LogicalTarget;
import java.util.Set;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Getter
public class RdfContainer<T extends Value> extends AbstractRdfMappedStatementCollection<T> {

    private final IRI type;

    private final T container;

    public static RdfContainer<Value> empty(IRI type, ValueFactory valueFactory, Set<LogicalTarget> logicalTargets) {
        var containerResource = valueFactory.createBNode();
        return RdfContainer.<Value>builder()
                .type(type)
                .container(containerResource)
                .logicalTargets(logicalTargets)
                .build();
    }

    @Override
    public T getValue() {
        return container;
    }

    @Override
    protected Stream<Statement> buildOwnStatements() {
        return RdfCollectionsAndContainers.toRdfContainerStatements(
                type, getElements(), (Resource) container, SimpleValueFactory.getInstance());
    }

    public RdfContainer<T> withGraphs(Set<MappedValue<Resource>> mappedGraphs) {
        var merged = mergeGraphs(mappedGraphs);
        return toBuilder()
                .logicalTargets(merged.logicalTargets())
                .graphs(merged.graphs())
                .build();
    }
}
