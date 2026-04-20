package io.carml.engine.rdf.cc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import io.carml.model.LogicalTarget;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class MergeableRdfListTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    void getKey_withoutGraphs_returnsHead() {
        // Given
        var head = VF.createBNode("head");
        var model = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("1")), head, VF);
        var list = MergeableRdfList.<Value>builder().head(head).model(model).build();

        // When
        var key = list.getKey();

        // Then
        assertThat(key, is(head));
    }

    @Test
    void getKey_withGraphs_returnsGraphScopedMergeKey() {
        // Given
        var head = VF.createBNode("head");
        var graph = Values.iri("http://example.com/g/1");
        var model = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("1")), head, VF);
        var list = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model)
                .graphs(Set.of(graph))
                .build();

        // When
        var key = list.getKey();

        // Then
        assertThat(key, instanceOf(GraphScopedMergeKey.class));
    }

    @Test
    void getKey_sameHeadDifferentGraphs_returnsDifferentKeys() {
        // Given
        var head = VF.createBNode("head");
        var graph1 = Values.iri("http://example.com/g/1");
        var graph2 = Values.iri("http://example.com/g/2");
        var model = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("1")), head, VF);

        var list1 = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model)
                .graphs(Set.of(graph1))
                .build();

        var list2 = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model)
                .graphs(Set.of(graph2))
                .build();

        // Then
        assertThat(list1.getKey(), not(is(list2.getKey())));
    }

    @Test
    void getKey_sameHeadSameGraphs_returnsSameKeys() {
        // Given
        var head = VF.createBNode("head");
        var graph = Values.iri("http://example.com/g/1");
        var model1 = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("1")), head, VF);
        var model2 = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("2")), head, VF);

        var list1 = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model1)
                .graphs(Set.of(graph))
                .build();

        var list2 = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model2)
                .graphs(Set.of(graph))
                .build();

        // Then
        assertThat(list1.getKey(), is(list2.getKey()));
    }

    @Test
    void merge_combinesValues_andPreservesGraphs() {
        // Given
        var head = VF.createBNode("head");
        var graph = Values.iri("http://example.com/g/1");
        var subject = Values.iri("http://example.com/e/a");
        var predicate = Values.iri("http://example.com/ns#with");

        var model1 = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("1")), head, VF);
        var model2 = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("3")), head, VF);

        var list1 = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model1)
                .graphs(Set.of(graph))
                .linkingSubjects(Set.of(subject))
                .linkingPredicates(Set.of(predicate))
                .build();

        var list2 = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model2)
                .graphs(Set.of(graph))
                .linkingSubjects(Set.of(subject))
                .linkingPredicates(Set.of(predicate))
                .build();

        // When
        var merged = list1.merge(list2);

        // Then
        var statements = Flux.from(merged.getResults()).collectList().block();
        assertNotNull(statements);
        assertThat(statements, is(not(List.of())));

        // Should have structural triples + linking triple, all in graph
        var firstValues = statements.stream()
                .filter(s -> s.getPredicate().equals(RDF.FIRST))
                .map(Statement::getObject)
                .toList();
        assertThat(firstValues, containsInAnyOrder(VF.createLiteral("1"), VF.createLiteral("3")));

        // All structural triples should be in the graph
        assertThat(statements.stream().allMatch(s -> graph.equals(s.getContext())), is(true));

        // Should include a linking triple
        var linkingTriples = statements.stream()
                .filter(s -> s.getSubject().equals(subject) && s.getPredicate().equals(predicate))
                .toList();
        assertThat(linkingTriples, hasSize(1));
    }

    @Test
    void merge_propagatesLogicalTargetsUnion() {
        // Given — two mergeable pieces each carrying distinct LogicalTargets. The merged result
        // must surface the union so that the post-merge observer wrap in
        // RdfRmlMapper#wrapMergedForObserver can route statements to every declared target.
        var head = VF.createBNode("head");
        var targetA = mock(LogicalTarget.class);
        var targetB = mock(LogicalTarget.class);

        var model1 = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("1")), head, VF);
        var model2 = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("2")), head, VF);

        var list1 = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model1)
                .logicalTarget(targetA)
                .build();

        var list2 = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model2)
                .logicalTarget(targetB)
                .build();

        // When
        var merged = list1.merge(list2);

        // Then — the merged result carries the union of both targets.
        assertThat(merged.getLogicalTargets(), is(Set.of(targetA, targetB)));
    }

    @Test
    void getResults_withGraphs_producesDistinctBlanksPerGraph() {
        // Given
        var head = VF.createBNode("head");
        var graph1 = Values.iri("http://example.com/g/1");
        var graph2 = Values.iri("http://example.com/g/2");
        var subject = Values.iri("http://example.com/e/a");
        var predicate = Values.iri("http://example.com/ns#with");

        var model = RdfCollectionsAndContainers.toRdfListModel(
                List.of(VF.createLiteral("1"), VF.createLiteral("2")), head, VF);

        var list = MergeableRdfList.<Value>builder()
                .head(head)
                .model(model)
                .graphs(Set.of(graph1, graph2))
                .linkingSubjects(Set.of(subject))
                .linkingPredicates(Set.of(predicate))
                .build();

        // When
        var statements = Flux.from(list.getResults()).collectList().block();

        // Then
        assertNotNull(statements);
        // Statements in graph1
        var g1Statements =
                statements.stream().filter(s -> graph1.equals(s.getContext())).toList();
        // Statements in graph2
        var g2Statements =
                statements.stream().filter(s -> graph2.equals(s.getContext())).toList();

        // Both graphs should have statements
        assertThat(g1Statements.isEmpty(), is(false));
        assertThat(g2Statements.isEmpty(), is(false));

        // Blank nodes in graph1 should be different from blank nodes in graph2
        var g1Blanks = extractBlanks(g1Statements);
        var g2Blanks = extractBlanks(g2Statements);
        g1Blanks.retainAll(g2Blanks);
        assertThat("Blank nodes should be distinct per graph", g1Blanks.isEmpty(), is(true));
    }

    @Test
    void getResults_withoutGraphs_returnsDefaultGraphStatements() {
        // Given
        var head = VF.createBNode("head");
        var model = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("1")), head, VF);

        var list = MergeableRdfList.<Value>builder().head(head).model(model).build();

        // When
        var statements = Flux.from(list.getResults()).collectList().block();

        // Then
        assertNotNull(statements);
        assertThat(statements.stream().allMatch(s -> s.getContext() == null), is(true));
    }

    @Test
    void withGraphScope_createsNewInstanceWithGraphInfo() {
        // Given
        var head = VF.createBNode("head");
        var graph = Values.iri("http://example.com/g/1");
        var subject = Values.iri("http://example.com/e/a");
        var predicate = Values.iri("http://example.com/ns#with");
        var model = RdfCollectionsAndContainers.toRdfListModel(List.of(VF.createLiteral("1")), head, VF);

        var list = MergeableRdfList.<Value>builder().head(head).model(model).build();

        // When
        var scoped = list.withGraphScope(Set.of(graph), Set.of(subject), Set.of(predicate));

        // Then
        assertThat(scoped.getGraphs(), is(Set.of(graph)));
        assertThat(scoped.getLinkingSubjects(), is(Set.of(subject)));
        assertThat(scoped.getLinkingPredicates(), is(Set.of(predicate)));
        assertThat(scoped.getHead(), is(head));
    }

    private List<BNode> extractBlanks(List<Statement> statements) {
        return statements.stream()
                .flatMap(stmt -> Stream.of(stmt.getSubject(), stmt.getObject()))
                .filter(BNode.class::isInstance)
                .map(BNode.class::cast)
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
