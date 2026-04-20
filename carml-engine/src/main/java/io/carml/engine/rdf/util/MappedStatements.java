package io.carml.engine.rdf.util;

import static org.eclipse.rdf4j.model.util.Values.getValueFactory;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.rdf.MappedStatement;
import io.carml.model.LogicalTarget;
import io.carml.output.NTriplesTermEncoder;
import io.carml.util.ModelsException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

@UtilityClass
public class MappedStatements {

    /** Produces the cartesian product of subjects x predicates x objects x graphs. */
    public static Stream<MappingResult<Statement>> streamCartesianProductMappedStatements(
            Set<MappedValue<Resource>> mappedSubjects,
            Set<MappedValue<IRI>> mappedPredicates,
            List<MappedValue<? extends Value>> mappedObjects,
            Set<MappedValue<Resource>> mappedGraphs) {
        return streamCartesianProductMappedStatements(
                mappedSubjects, mappedPredicates, mappedObjects, mappedGraphs, graph -> graph, getValueFactory());
    }

    @SafeVarargs
    public static Stream<MappingResult<Statement>> streamCartesianProductMappedStatements(
            Set<MappedValue<Resource>> mappedSubjects,
            Set<MappedValue<IRI>> mappedPredicates,
            List<MappedValue<? extends Value>> mappedObjects,
            Set<MappedValue<Resource>> mappedGraphs,
            UnaryOperator<Resource> graphModifier,
            ValueFactory valueFactory,
            Consumer<Statement>... statementConsumers) {
        if (mappedSubjects.isEmpty() || mappedPredicates.isEmpty() || mappedObjects.isEmpty()) {
            throw new ModelsException("Could not create cartesian product statements because at least one of subjects,"
                    + " predicates or objects was empty.");
        }

        if (mappedGraphs.isEmpty()) {
            return mappedSubjects.stream()
                    .flatMap(subject -> mappedPredicates.stream()
                            .flatMap(predicate -> mappedObjects.stream()
                                    .map(object -> createMappedStatement(
                                            subject,
                                            predicate,
                                            object,
                                            null,
                                            graphModifier,
                                            valueFactory,
                                            statementConsumers))));
        } else {
            return mappedSubjects.stream()
                    .flatMap(subject -> mappedPredicates.stream()
                            .flatMap(predicate -> mappedObjects.stream()
                                    .flatMap(object -> mappedGraphs.stream()
                                            .map(graph -> createMappedStatement(
                                                    subject,
                                                    predicate,
                                                    object,
                                                    graph,
                                                    graphModifier,
                                                    valueFactory,
                                                    statementConsumers)))));
        }
    }

    /**
     * Produces the cartesian product of subjects x predicates x objects x graphs and encodes each
     * combination directly to N-Triples or N-Quads bytes, bypassing {@link Statement} creation.
     *
     * @param mappedSubjects the subject terms
     * @param mappedPredicates the predicate terms
     * @param mappedObjects the object terms
     * @param mappedGraphs the graph terms (empty for N-Triples / default graph)
     * @param graphModifier modifier applied to each graph resource (e.g. defaultGraph filter)
     * @param encoder the encoder to use for byte serialization
     * @param includeGraph whether to include the graph field in encoded output (true for N-Quads,
     *     false for N-Triples)
     * @return a stream of encoded byte arrays, one per triple/quad line
     */
    public static Stream<byte[]> streamCartesianProductBytes(
            Set<MappedValue<Resource>> mappedSubjects,
            Set<MappedValue<IRI>> mappedPredicates,
            List<MappedValue<? extends Value>> mappedObjects,
            Set<MappedValue<Resource>> mappedGraphs,
            UnaryOperator<Resource> graphModifier,
            NTriplesTermEncoder encoder,
            boolean includeGraph) {
        if (mappedSubjects.isEmpty() || mappedPredicates.isEmpty() || mappedObjects.isEmpty()) {
            throw new ModelsException("Could not create cartesian product bytes because at least one of subjects,"
                    + " predicates or objects was empty.");
        }

        if (!includeGraph || mappedGraphs.isEmpty()) {
            return mappedSubjects.stream()
                    .flatMap(subject -> mappedPredicates.stream()
                            .flatMap(predicate -> mappedObjects.stream()
                                    .map(object -> encoder.encodeNTriple(
                                            subject.getValue(), predicate.getValue(), object.getValue()))));
        } else {
            return mappedSubjects.stream()
                    .flatMap(subject -> mappedPredicates.stream()
                            .flatMap(predicate -> mappedObjects.stream()
                                    .flatMap(object -> mappedGraphs.stream().map(graph -> {
                                        var modifiedGraph = graphModifier.apply(graph.getValue());
                                        return encoder.encodeNQuad(
                                                subject.getValue(),
                                                predicate.getValue(),
                                                object.getValue(),
                                                modifiedGraph);
                                    }))));
        }
    }

    @SafeVarargs
    public static MappingResult<Statement> createMappedStatement(
            @NonNull MappedValue<? extends Value> subjectValue,
            @NonNull MappedValue<? extends Value> predicateValue,
            @NonNull MappedValue<? extends Value> objectValue,
            MappedValue<? extends Value> graphValue,
            @NonNull UnaryOperator<Resource> graphModifier,
            @NonNull ValueFactory valueFactory,
            Consumer<Statement>... statementConsumers) {
        Resource subject = (Resource) subjectValue.getValue();
        IRI predicate = (IRI) predicateValue.getValue();
        Value object = objectValue.getValue();
        Resource graph = graphValue != null ? graphModifier.apply((Resource) graphValue.getValue()) : null;

        var statement = valueFactory.createStatement(subject, predicate, object, graph);

        for (Consumer<Statement> statementConsumer : statementConsumers) {
            statementConsumer.accept(statement);
        }

        var graphTargets = graphValue != null ? graphValue.getLogicalTargets() : Set.<LogicalTarget>of();

        var logicalTargets = Stream.of(
                        subjectValue.getLogicalTargets(),
                        predicateValue.getLogicalTargets(),
                        objectValue.getLogicalTargets(),
                        graphTargets)
                .flatMap(Set::stream)
                .collect(Collectors.toUnmodifiableSet());

        return MappedStatement.builder() //
                .statement(statement)
                .logicalTargets(logicalTargets)
                .build();
    }
}
