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
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class MergeableRdfContainerTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    void getKey_withoutGraphs_returnsContainer() {
        // Given
        var container = VF.createBNode("container");
        var model =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("1")), container, VF);
        var mergeableContainer = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model)
                .build();

        // When
        var key = mergeableContainer.getKey();

        // Then
        assertThat(key, is(container));
    }

    @Test
    void getKey_withGraphs_returnsGraphScopedMergeKey() {
        // Given
        var container = VF.createBNode("container");
        var graph = Values.iri("http://example.com/g/1");
        var model =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("1")), container, VF);
        var mergeableContainer = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model)
                .graphs(Set.of(graph))
                .build();

        // When
        var key = mergeableContainer.getKey();

        // Then
        assertThat(key, instanceOf(GraphScopedMergeKey.class));
    }

    @Test
    void getKey_sameContainerDifferentGraphs_returnsDifferentKeys() {
        // Given
        var container = VF.createBNode("container");
        var graph1 = Values.iri("http://example.com/g/1");
        var graph2 = Values.iri("http://example.com/g/2");
        var model =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("1")), container, VF);

        var c1 = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model)
                .graphs(Set.of(graph1))
                .build();

        var c2 = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model)
                .graphs(Set.of(graph2))
                .build();

        // Then
        assertThat(c1.getKey(), not(is(c2.getKey())));
    }

    @Test
    void getKey_sameContainerSameGraphs_returnsSameKeys() {
        // Given
        var container = VF.createBNode("container");
        var graph = Values.iri("http://example.com/g/1");
        var model1 =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("1")), container, VF);
        var model2 =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("2")), container, VF);

        var c1 = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model1)
                .graphs(Set.of(graph))
                .build();

        var c2 = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model2)
                .graphs(Set.of(graph))
                .build();

        // Then
        assertThat(c1.getKey(), is(c2.getKey()));
    }

    @Test
    void merge_combinesValues_andPreservesGraphs() {
        // Given
        var container = VF.createBNode("container");
        var graph = Values.iri("http://example.com/g/1");
        var subject = Values.iri("http://example.com/e/a");
        var predicate = Values.iri("http://example.com/ns#with");

        var model1 =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("1")), container, VF);
        var model2 =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("3")), container, VF);

        var c1 = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model1)
                .graphs(Set.of(graph))
                .linkingSubjects(Set.of(subject))
                .linkingPredicates(Set.of(predicate))
                .build();

        var c2 = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model2)
                .graphs(Set.of(graph))
                .linkingSubjects(Set.of(subject))
                .linkingPredicates(Set.of(predicate))
                .build();

        // When
        var merged = c1.merge(c2);

        // Then
        var statements = Flux.from(merged.getResults()).collectList().block();
        assertNotNull(statements);
        assertThat(statements, is(not(List.of())));

        // Should have rdf:type triple + two member triples + linking triple, all in graph
        var memberValues = statements.stream()
                .filter(s -> s.getPredicate().stringValue().startsWith(RDF.NAMESPACE + "_"))
                .map(Statement::getObject)
                .toList();
        assertThat(memberValues, containsInAnyOrder(VF.createLiteral("1"), VF.createLiteral("3")));

        // All triples should be in the graph
        assertThat(statements.stream().allMatch(s -> graph.equals(s.getContext())), is(true));

        // Should include a linking triple
        var linkingTriples = statements.stream()
                .filter(s -> s.getSubject().equals(subject) && s.getPredicate().equals(predicate))
                .toList();
        assertThat(linkingTriples, hasSize(1));
    }

    @Test
    void merge_propagatesLogicalTargetsUnion() {
        // Given — two mergeable container pieces each carrying distinct LogicalTargets. The
        // merged result must surface the union so that the post-merge observer wrap in
        // RdfRmlMapper#wrapMergedForObserver can route statements to every declared target.
        var container = VF.createBNode("container");
        var targetA = mock(LogicalTarget.class);
        var targetB = mock(LogicalTarget.class);

        var model1 =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("1")), container, VF);
        var model2 =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("2")), container, VF);

        var c1 = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model1)
                .logicalTarget(targetA)
                .build();

        var c2 = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model2)
                .logicalTarget(targetB)
                .build();

        // When
        var merged = c1.merge(c2);

        // Then — the merged result carries the union of both targets.
        assertThat(merged.getLogicalTargets(), is(Set.of(targetA, targetB)));
    }

    @Test
    void getResults_withGraphs_producesDistinctBlanksPerGraph() {
        // Given
        var container = VF.createBNode("container");
        var graph1 = Values.iri("http://example.com/g/1");
        var graph2 = Values.iri("http://example.com/g/2");
        var subject = Values.iri("http://example.com/e/a");
        var predicate = Values.iri("http://example.com/ns#with");

        var model = RdfCollectionsAndContainers.toRdfContainerModel(
                RDF.BAG, List.of(VF.createLiteral("1"), VF.createLiteral("2")), container, VF);

        var mergeableContainer = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model)
                .graphs(Set.of(graph1, graph2))
                .linkingSubjects(Set.of(subject))
                .linkingPredicates(Set.of(predicate))
                .build();

        // When
        var statements =
                Flux.from(mergeableContainer.getResults()).collectList().block();

        // Then
        assertNotNull(statements);
        var g1Statements =
                statements.stream().filter(s -> graph1.equals(s.getContext())).toList();
        var g2Statements =
                statements.stream().filter(s -> graph2.equals(s.getContext())).toList();

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
        var container = VF.createBNode("container");
        var model =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("1")), container, VF);

        var mergeableContainer = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model)
                .build();

        // When
        var statements =
                Flux.from(mergeableContainer.getResults()).collectList().block();

        // Then
        assertNotNull(statements);
        assertThat(statements.stream().allMatch(s -> s.getContext() == null), is(true));
    }

    @Test
    void withGraphScope_createsNewInstanceWithGraphInfo() {
        // Given
        var container = VF.createBNode("container");
        var graph = Values.iri("http://example.com/g/1");
        var subject = Values.iri("http://example.com/e/a");
        var predicate = Values.iri("http://example.com/ns#with");
        var model =
                RdfCollectionsAndContainers.toRdfContainerModel(RDF.BAG, List.of(VF.createLiteral("1")), container, VF);

        var mergeableContainer = MergeableRdfContainer.<Value>builder()
                .type(RDF.BAG)
                .container(container)
                .model(model)
                .build();

        // When
        var scoped = mergeableContainer.withGraphScope(Set.of(graph), Set.of(subject), Set.of(predicate));

        // Then
        assertThat(scoped.getGraphs(), is(Set.of(graph)));
        assertThat(scoped.getLinkingSubjects(), is(Set.of(subject)));
        assertThat(scoped.getLinkingPredicates(), is(Set.of(predicate)));
        assertThat(scoped.getContainer(), is(container));
        assertThat(scoped.getType(), is(RDF.BAG));
        assertThat(scoped.getLogicalTargets(), is(Set.of()));
    }

    private List<BNode> extractBlanks(List<Statement> statements) {
        var blanks = new ArrayList<BNode>();
        for (Statement stmt : statements) {
            if (stmt.getSubject() instanceof BNode bnode) {
                blanks.add(bnode);
            }
            if (stmt.getObject() instanceof BNode bnode) {
                blanks.add(bnode);
            }
        }
        return blanks;
    }
}
