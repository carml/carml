package io.carml.engine.rdf.cc;

import io.carml.engine.MappedValue;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

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

    public RdfList<T> withGraphs(Set<MappedValue<Resource>> mappedGraphs) {
        var merged = mergeGraphs(mappedGraphs);
        return toBuilder()
                .logicalTargets(merged.logicalTargets())
                .model(merged.model())
                .build();
    }
}
