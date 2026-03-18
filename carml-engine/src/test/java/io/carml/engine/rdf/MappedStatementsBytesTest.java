package io.carml.engine.rdf;

import static io.carml.engine.rdf.util.MappedStatements.streamCartesianProductBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.engine.MappedValue;
import io.carml.output.NTriplesTermEncoder;
import io.carml.util.ModelsException;
import io.carml.vocab.Rdf;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;

class MappedStatementsBytesTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private final NTriplesTermEncoder encoder = NTriplesTermEncoder.withDefaults();

    private static final UnaryOperator<Resource> IDENTITY_GRAPH_MODIFIER = graph -> graph;

    @Test
    void streamCartesianProductBytes_singleSubjectPredicateObject_noGraph_producesOneNTLine() {
        Set<MappedValue<Resource>> subjects = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/s")));
        Set<MappedValue<IRI>> predicates = Set.of(RdfMappedValue.of(RDF.TYPE));
        List<MappedValue<? extends Value>> objects = List.of(RdfMappedValue.of(VF.createIRI("http://example.org/T")));
        Set<MappedValue<Resource>> graphs = Set.of();

        var bytes = streamCartesianProductBytes(
                        subjects, predicates, objects, graphs, IDENTITY_GRAPH_MODIFIER, encoder, false)
                .toList();

        assertThat(bytes, hasSize(1));
        var line = new String(bytes.get(0), StandardCharsets.UTF_8);
        assertThat(
                line,
                is("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/T> .\n"));
    }

    @Test
    void streamCartesianProductBytes_withGraph_includeGraphTrue_producesNQLine() {
        Set<MappedValue<Resource>> subjects = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/s")));
        Set<MappedValue<IRI>> predicates = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/p")));
        List<MappedValue<? extends Value>> objects = List.of(RdfMappedValue.of(VF.createLiteral("value")));
        Set<MappedValue<Resource>> graphs = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/g")));

        var bytes = streamCartesianProductBytes(
                        subjects, predicates, objects, graphs, IDENTITY_GRAPH_MODIFIER, encoder, true)
                .toList();

        assertThat(bytes, hasSize(1));
        var line = new String(bytes.get(0), StandardCharsets.UTF_8);
        assertThat(line, containsString("<http://example.org/g>"));
        assertThat(line, containsString("\"value\""));
    }

    @Test
    void streamCartesianProductBytes_withGraph_includeGraphFalse_producesNTLine() {
        Set<MappedValue<Resource>> subjects = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/s")));
        Set<MappedValue<IRI>> predicates = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/p")));
        List<MappedValue<? extends Value>> objects = List.of(RdfMappedValue.of(VF.createLiteral("value")));
        Set<MappedValue<Resource>> graphs = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/g")));

        var bytes = streamCartesianProductBytes(
                        subjects, predicates, objects, graphs, IDENTITY_GRAPH_MODIFIER, encoder, false)
                .toList();

        assertThat(bytes, hasSize(1));
        var line = new String(bytes.get(0), StandardCharsets.UTF_8);
        // Graph present but suppressed: no graph IRI in output
        assertThat(line, not(containsString("<http://example.org/g>")));
        assertThat(line, containsString("\"value\" .\n"));
    }

    @Test
    void streamCartesianProductBytes_multipleObjects_producesMultipleLines() {
        Set<MappedValue<Resource>> subjects = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/s")));
        Set<MappedValue<IRI>> predicates = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/p")));
        List<MappedValue<? extends Value>> objects =
                List.of(RdfMappedValue.of(VF.createLiteral("one")), RdfMappedValue.of(VF.createLiteral("two")));
        Set<MappedValue<Resource>> graphs = Set.of();

        var bytes = streamCartesianProductBytes(
                        subjects, predicates, objects, graphs, IDENTITY_GRAPH_MODIFIER, encoder, false)
                .toList();

        assertThat(bytes, hasSize(2));
        var line1 = new String(bytes.get(0), StandardCharsets.UTF_8);
        var line2 = new String(bytes.get(1), StandardCharsets.UTF_8);
        assertThat(line1, containsString("\"one\""));
        assertThat(line2, containsString("\"two\""));
    }

    @Test
    void streamCartesianProductBytes_multipleSubjectsAndObjects_producesCartesianProduct() {
        // Use LinkedHashSet for deterministic iteration order
        var subjects = new LinkedHashSet<MappedValue<Resource>>();
        subjects.add(RdfMappedValue.of(VF.createIRI("http://example.org/s1")));
        subjects.add(RdfMappedValue.of(VF.createIRI("http://example.org/s2")));

        Set<MappedValue<IRI>> predicates = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/p")));

        List<MappedValue<? extends Value>> objects =
                List.of(RdfMappedValue.of(VF.createLiteral("a")), RdfMappedValue.of(VF.createLiteral("b")));

        Set<MappedValue<Resource>> graphs = Set.of();

        var bytes = streamCartesianProductBytes(
                        subjects, predicates, objects, graphs, IDENTITY_GRAPH_MODIFIER, encoder, false)
                .toList();

        // 2 subjects x 1 predicate x 2 objects = 4 lines
        assertThat(bytes, hasSize(4));

        var allLines =
                bytes.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();

        // Verify all subject/object combinations are present
        var s1a = allLines.stream().anyMatch(l -> l.contains("<http://example.org/s1>") && l.contains("\"a\""));
        var s1b = allLines.stream().anyMatch(l -> l.contains("<http://example.org/s1>") && l.contains("\"b\""));
        var s2a = allLines.stream().anyMatch(l -> l.contains("<http://example.org/s2>") && l.contains("\"a\""));
        var s2b = allLines.stream().anyMatch(l -> l.contains("<http://example.org/s2>") && l.contains("\"b\""));

        assertThat(s1a, is(true));
        assertThat(s1b, is(true));
        assertThat(s2a, is(true));
        assertThat(s2b, is(true));
    }

    @Test
    void streamCartesianProductBytes_defaultGraphModifier_nullifiesDefaultGraph() {
        // Use the same defaultGraphModifier from RdfTriplesMapper that converts
        // Rml.defaultGraph / Rr.defaultGraph to null
        UnaryOperator<Resource> defaultGraphModifier = RdfTriplesMapper.defaultGraphModifier;

        Set<MappedValue<Resource>> subjects = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/s")));
        Set<MappedValue<IRI>> predicates = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/p")));
        List<MappedValue<? extends Value>> objects = List.of(RdfMappedValue.of(VF.createLiteral("value")));
        Set<MappedValue<Resource>> graphs = Set.of(RdfMappedValue.of(Rdf.Rml.defaultGraph));

        var bytes = streamCartesianProductBytes(
                        subjects, predicates, objects, graphs, defaultGraphModifier, encoder, true)
                .toList();

        assertThat(bytes, hasSize(1));
        var line = new String(bytes.get(0), StandardCharsets.UTF_8);
        // defaultGraph is mapped to null by the modifier, so no graph field should appear
        // encodeNQuad with null graph produces NT output (no graph field)
        assertThat(line, not(containsString("defaultGraph")));
        assertThat(line, is("<http://example.org/s> <http://example.org/p> \"value\" .\n"));
    }

    @Test
    void streamCartesianProductBytes_emptySubjects_throwsModelsException() {
        Set<MappedValue<Resource>> subjects = Set.of();
        Set<MappedValue<IRI>> predicates = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/p")));
        List<MappedValue<? extends Value>> objects = List.of(RdfMappedValue.of(VF.createLiteral("v")));
        Set<MappedValue<Resource>> graphs = Set.of();

        assertThrows(
                ModelsException.class,
                () -> streamCartesianProductBytes(
                        subjects, predicates, objects, graphs, IDENTITY_GRAPH_MODIFIER, encoder, false));
    }

    @Test
    void streamCartesianProductBytes_emptyPredicates_throwsModelsException() {
        Set<MappedValue<Resource>> subjects = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/s")));
        Set<MappedValue<IRI>> predicates = Set.of();
        List<MappedValue<? extends Value>> objects = List.of(RdfMappedValue.of(VF.createLiteral("v")));
        Set<MappedValue<Resource>> graphs = Set.of();

        assertThrows(
                ModelsException.class,
                () -> streamCartesianProductBytes(
                        subjects, predicates, objects, graphs, IDENTITY_GRAPH_MODIFIER, encoder, false));
    }

    @Test
    void streamCartesianProductBytes_emptyObjects_throwsModelsException() {
        Set<MappedValue<Resource>> subjects = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/s")));
        Set<MappedValue<IRI>> predicates = Set.of(RdfMappedValue.of(VF.createIRI("http://example.org/p")));
        List<MappedValue<? extends Value>> objects = List.of();
        Set<MappedValue<Resource>> graphs = Set.of();

        assertThrows(
                ModelsException.class,
                () -> streamCartesianProductBytes(
                        subjects, predicates, objects, graphs, IDENTITY_GRAPH_MODIFIER, encoder, false));
    }
}
