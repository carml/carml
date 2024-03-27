package io.carml.engine.rdf;

import io.carml.engine.MergeableMappingResult;
import io.carml.engine.rdf.util.RdfCollectionsAndContainers;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.RDFContainers;

@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString
@Getter
public class MergeableRdfContainer<T extends Value> extends RdfContainer<T>
        implements MergeableMappingResult<Value, Statement> {

    @Override
    public Value getKey() {
        return getContainer();
    }

    @SuppressWarnings("unchecked")
    @Override
    public MergeableMappingResult<Value, Statement> merge(MergeableMappingResult<Value, Statement> other) {
        return MergeableRdfContainer.builder()
                .type(getType())
                .container(getContainer())
                .targets(getTargets())
                .model(concatenateContainer((T) other.getKey(), ((MergeableRdfContainer<T>) other).getModel()))
                .build();
    }

    private Model concatenateContainer(T otherContainer, Model other) {
        var thisList = containerToValueList((Resource) getContainer(), getModel());
        var otherList = containerToValueList((Resource) otherContainer, other);

        thisList.addAll(otherList);

        return RdfCollectionsAndContainers.toRdfContainerModel(
                getType(), thisList, (Resource) getContainer(), SimpleValueFactory.getInstance());
    }

    private List<Value> containerToValueList(Resource container, Model model) {
        var list = new ArrayList<Statement>();
        RDFContainers.extract(getType(), model, container, list::add);

        return list.stream().map(Statement::getObject).collect(Collectors.toCollection(ArrayList::new));
    }
}
