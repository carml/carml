package io.carml.engine.rdf;

import io.carml.engine.MergeableMappingResult;
import io.carml.engine.rdf.util.RdfCollectionsAndContainers;
import io.carml.util.Models;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.RDFContainers;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
@ToString
@Getter
public class MergeableRdfContainer<T extends Value> extends RdfContainer<T>
        implements MergeableMappingResult<Value, Statement> {

    /**
     * The set of graph resources this container is scoped to. Empty means default graph.
     */
    @Builder.Default
    private final Set<Resource> graphs = Set.of();

    /**
     * Subjects for generating linking triples ({@code subject predicate container graph}).
     */
    @Builder.Default
    private final Set<Resource> linkingSubjects = Set.of();

    /**
     * Predicates for generating linking triples ({@code subject predicate container graph}).
     */
    @Builder.Default
    private final Set<IRI> linkingPredicates = Set.of();

    @Override
    public Value getKey() {
        if (graphs.isEmpty()) {
            return getContainer();
        }
        return new GraphScopedMergeKey(getContainer(), graphs);
    }

    @SuppressWarnings("unchecked")
    @Override
    public MergeableMappingResult<Value, Statement> merge(MergeableMappingResult<Value, Statement> other) {
        var otherContainer = (MergeableRdfContainer<T>) other;

        var mergedLinkingSubjects = new HashSet<>(linkingSubjects);
        mergedLinkingSubjects.addAll(otherContainer.getLinkingSubjects());

        var mergedLinkingPredicates = new HashSet<>(linkingPredicates);
        mergedLinkingPredicates.addAll(otherContainer.getLinkingPredicates());

        return toBuilder()
                .model(concatenateContainer((T) GraphScopedMergeKey.unwrap(other.getKey()), otherContainer.getModel()))
                .linkingSubjects(Set.copyOf(mergedLinkingSubjects))
                .linkingPredicates(Set.copyOf(mergedLinkingPredicates))
                .build();
    }

    @Override
    public Publisher<Statement> getResults() {
        if (graphs.isEmpty()) {
            return Flux.fromIterable(getModel());
        }

        return Flux.fromIterable(buildGraphScopedStatements());
    }

    private List<Statement> buildGraphScopedStatements() {
        var valueFactory = SimpleValueFactory.getInstance();

        return graphs.stream()
                .flatMap(graph -> {
                    var remapResult = Models.remapBlanksForGraph(getModel(), graph, valueFactory);
                    var bnodeMap = remapResult.getKey();
                    var graphModel = remapResult.getValue();

                    var graphStatements = graphModel.stream();

                    if (!linkingSubjects.isEmpty() && !linkingPredicates.isEmpty()) {
                        var remappedContainer = getContainer() instanceof BNode containerBNode
                                ? bnodeMap.getOrDefault(containerBNode, containerBNode)
                                : getContainer();

                        var linkingTriples = linkingSubjects.stream().flatMap(subject -> linkingPredicates.stream()
                                .map(predicate ->
                                        valueFactory.createStatement(subject, predicate, remappedContainer, graph)));

                        return Stream.concat(graphStatements, linkingTriples);
                    }

                    return graphStatements;
                })
                .toList();
    }

    private Model concatenateContainer(T otherContainerValue, Model other) {
        var thisList = containerToValueList((Resource) getContainer(), getModel());
        var otherList = containerToValueList((Resource) otherContainerValue, other);

        thisList.addAll(otherList);

        return RdfCollectionsAndContainers.toRdfContainerModel(
                getType(), thisList, (Resource) getContainer(), SimpleValueFactory.getInstance());
    }

    private List<Value> containerToValueList(Resource container, Model model) {
        var list = new ArrayList<Statement>();
        RDFContainers.extract(getType(), model, container, list::add);

        return list.stream().map(Statement::getObject).collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * Creates a new {@link MergeableRdfContainer} scoped to the given graphs with linking triple info.
     * The model is kept in the default graph context; graph scoping with fresh blank nodes is applied
     * when {@link #getResults()} is called (after merging).
     */
    public MergeableRdfContainer<T> withGraphScope(
            Set<Resource> targetGraphs, Set<Resource> subjects, Set<IRI> predicates) {
        return toBuilder()
                .graphs(targetGraphs)
                .linkingSubjects(subjects)
                .linkingPredicates(predicates)
                .build();
    }
}
