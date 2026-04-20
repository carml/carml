package io.carml.engine.rdf.cc;

import io.carml.engine.MergeableMappingResult;
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
import org.eclipse.rdf4j.model.util.RDFCollections;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
@ToString
@Getter
public class MergeableRdfList<T extends Value> extends RdfList<T> implements MergeableMappingResult<Value, Statement> {

    /**
     * The set of graph resources this list is scoped to. Empty means default graph.
     */
    @Builder.Default
    private final Set<Resource> graphs = Set.of();

    /**
     * Subjects for generating linking triples ({@code subject predicate head graph}).
     */
    @Builder.Default
    private final Set<Resource> linkingSubjects = Set.of();

    /**
     * Predicates for generating linking triples ({@code subject predicate head graph}).
     */
    @Builder.Default
    private final Set<IRI> linkingPredicates = Set.of();

    @Override
    public Value getKey() {
        if (graphs.isEmpty()) {
            return getHead();
        }
        return new GraphScopedMergeKey(getHead(), graphs);
    }

    @SuppressWarnings("unchecked")
    @Override
    public MergeableMappingResult<Value, Statement> merge(MergeableMappingResult<Value, Statement> other) {
        var otherList = (MergeableRdfList<T>) other;

        var mergedLinkingSubjects = new HashSet<>(linkingSubjects);
        mergedLinkingSubjects.addAll(otherList.getLinkingSubjects());

        var mergedLinkingPredicates = new HashSet<>(linkingPredicates);
        mergedLinkingPredicates.addAll(otherList.getLinkingPredicates());

        // Propagate logicalTargets union: toBuilder() pre-fills with this instance's targets;
        // .logicalTargets(Iterable) adds (not replaces) under @Singular, so the resulting builder
        // holds the union of both sides. The observer-firing wrap at RmlMapper.mergeMergeables()
        // relies on this union being surfaced to onStatementGenerated.
        return toBuilder()
                .model(concatenateCollection((T) GraphScopedMergeKey.unwrap(other.getKey()), otherList.getModel()))
                .logicalTargets(otherList.getLogicalTargets())
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
                        var remappedHead = getHead() instanceof BNode headBNode
                                ? bnodeMap.getOrDefault(headBNode, headBNode)
                                : getHead();

                        var linkingTriples = linkingSubjects.stream()
                                .flatMap(subject -> linkingPredicates.stream()
                                        .map(predicate ->
                                                valueFactory.createStatement(subject, predicate, remappedHead, graph)));

                        return Stream.concat(graphStatements, linkingTriples);
                    }

                    return graphStatements;
                })
                .toList();
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

    /**
     * Creates a new {@link MergeableRdfList} scoped to the given graphs with linking triple info.
     * The model is kept in the default graph context; graph scoping with fresh blank nodes is applied
     * when {@link #getResults()} is called (after merging).
     */
    public MergeableRdfList<T> withGraphScope(Set<Resource> targetGraphs, Set<Resource> subjects, Set<IRI> predicates) {
        return toBuilder()
                .graphs(targetGraphs)
                .linkingSubjects(subjects)
                .linkingPredicates(predicates)
                .build();
    }
}
