package io.carml.engine.rdf.cc;

import io.carml.engine.MappedValue;
import io.carml.model.LogicalTarget;
import java.util.List;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Getter
public class RdfContainer<T extends Value> extends AbstractRdfMappedStatementCollection<T> {

    private final IRI type;

    private final T container;

    public static RdfContainer<Value> empty(IRI type, ValueFactory valueFactory, Set<LogicalTarget> logicalTargets) {
        var containerResource = valueFactory.createBNode();
        var model = RdfCollectionsAndContainers.toRdfContainerModel(type, List.of(), containerResource, valueFactory);
        return RdfContainer.<Value>builder()
                .type(type)
                .container(containerResource)
                .model(model)
                .logicalTargets(logicalTargets)
                .build();
    }

    @Override
    public T getValue() {
        return container;
    }

    public RdfContainer<T> withGraphs(Set<MappedValue<Resource>> mappedGraphs) {
        var merged = mergeGraphs(mappedGraphs);
        return toBuilder()
                .logicalTargets(merged.logicalTargets())
                .model(merged.model())
                .build();
    }
}
