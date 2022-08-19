package io.carml.util.jena;

import static io.carml.util.jena.JenaCollectors.toDataset;
import static io.carml.util.jena.JenaCollectors.toDatasetGraph;
import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.rdf.model.ResourceFactory.createPlainLiteral;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.jena.sparql.core.Quad.defaultGraphIRI;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.sparql.core.Quad;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

class JenaCollectorsTest {

  @Test
  void givenStatements_whenToDatasetGraph_thenReturnsDatasetGraph() {
    // Given
    var quads = getTestQuads();

    // When
    var datasetGraph = quads.collect(toDatasetGraph());

    // Then
    datasetGraph.listGraphNodes()
        .forEachRemaining(node -> assertThat(node.getURI(),
            in(List.of("http://example.org/graph1", "http://example.org/graph2", defaultGraphIRI.getURI()))));

    var graph1 = datasetGraph.getGraph(createURI("http://example.org/graph1"));
    assertThat(graph1.stream()
        .count(), is(2L));
    assertThat(graph1.contains(createURI("http://example.org/subject1"), createURI(RDF.TYPE.stringValue()),
        createURI(RDFS.RESOURCE.stringValue())), is(true));
    assertThat(graph1.contains(createURI("http://example.org/subject1"), createURI(RDFS.LABEL.stringValue()),
        createLiteral("subject1")), is(true));

    var graph2 = datasetGraph.getGraph(createURI("http://example.org/graph2"));
    assertThat(graph2.stream()
        .count(), is(2L));
    assertThat(graph2.contains(createURI("http://example.org/subject2"), createURI(RDF.TYPE.stringValue()),
        createURI(RDFS.RESOURCE.stringValue())), is(true));
    assertThat(graph2.contains(createURI("http://example.org/subject2"), createURI(RDFS.LABEL.stringValue()),
        createLiteral("subject2")), is(true));

    var defaultGraph = datasetGraph.getDefaultGraph();
    assertThat(defaultGraph.stream()
        .count(), is(2L));
    assertThat(defaultGraph.contains(createURI("http://example.org/subject"), createURI(RDF.TYPE.stringValue()),
        createURI(RDFS.RESOURCE.stringValue())), is(true));
    assertThat(defaultGraph.contains(createURI("http://example.org/subject"), createURI(RDFS.LABEL.stringValue()),
        createLiteral("subject")), is(true));
  }

  @Test
  void givenStatements_whenToDataset_thenReturnsDataset() {
    // Given
    var quads = getTestQuads();

    // When
    var dataset = quads.collect(toDataset());

    // Then
    dataset.listModelNames()
        .forEachRemaining(node -> assertThat(node.getURI(),
            in(List.of("http://example.org/graph1", "http://example.org/graph2", defaultGraphIRI.getURI()))));

    var model1 = dataset.getNamedModel("http://example.org/graph1");
    assertThat(model1.size(), is(2L));
    assertThat(model1.contains(createResource("http://example.org/subject1"), createProperty(RDF.TYPE.stringValue()),
        createResource(RDFS.RESOURCE.stringValue())), is(true));
    assertThat(model1.contains(createResource("http://example.org/subject1"), createProperty(RDFS.LABEL.stringValue()),
        createPlainLiteral("subject1")), is(true));

    var model2 = dataset.getNamedModel("http://example.org/graph2");
    assertThat(model2.size(), is(2L));
    assertThat(model2.contains(createResource("http://example.org/subject2"), createProperty(RDF.TYPE.stringValue()),
        createResource(RDFS.RESOURCE.stringValue())), is(true));
    assertThat(model2.contains(createResource("http://example.org/subject2"), createProperty(RDFS.LABEL.stringValue()),
        createPlainLiteral("subject2")), is(true));

    var defaultModel = dataset.getDefaultModel();
    assertThat(defaultModel.size(), is(2L));
    assertThat(defaultModel.contains(createResource("http://example.org/subject"),
        createProperty(RDF.TYPE.stringValue()), createResource(RDFS.RESOURCE.stringValue())), is(true));
    assertThat(defaultModel.contains(createResource("http://example.org/subject"),
        createProperty(RDFS.LABEL.stringValue()), createPlainLiteral("subject")), is(true));
  }

  private Stream<Quad> getTestQuads() {
    return new ModelBuilder().setNamespace("ex", "http://example.org/")
        .namedGraph("ex:graph1")
        .subject("ex:subject1")
        .add(RDF.TYPE, RDFS.RESOURCE)
        .add(RDFS.LABEL, "subject1")
        .namedGraph("ex:graph2")
        .subject("ex:subject2")
        .add(RDF.TYPE, RDFS.RESOURCE)
        .add(RDFS.LABEL, "subject2")
        .defaultGraph()
        .subject("ex:subject")
        .add(RDF.TYPE, RDFS.RESOURCE)
        .add(RDFS.LABEL, "subject")
        .build()
        .parallelStream()
        .map(JenaConverters::toQuad);
  }
}
