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
import org.eclipse.rdf4j.model.util.RDFCollections;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString
@Getter
public class MergeableRdfList<T extends Value> extends RdfList<T> implements MergeableMappingResult<Value, Statement> {

    @Override
    public Value getKey() {
        return getHead();
    }

    @SuppressWarnings("unchecked")
    @Override
    public MergeableMappingResult<Value, Statement> merge(MergeableMappingResult<Value, Statement> other) {
        return MergeableRdfList.builder()
                .head(getHead())
                .targets(getTargets())
                .model(concatenateCollection((T) other.getKey(), ((MergeableRdfList<T>) other).getModel()))
                .build();
    }

    private Model concatenateCollection(T otherHead, Model other) {
        var thisList = collectionToValueList((Resource) getHead(), getModel());
        var otherList = collectionToValueList((Resource) otherHead, other);

        thisList.addAll(otherList);

        return RdfCollectionsAndContainers.toRdfListModel(
                thisList, (Resource) getHead(), SimpleValueFactory.getInstance());
    }

    private List<Value> collectionToValueList(Resource head, Model model) {
        var list = new ArrayList<Statement>();
        RDFCollections.extract(model, head, list::add);

        return list.stream()
                .filter(statement -> statement.getPredicate().equals(RDF.FIRST))
                .map(Statement::getObject)
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
