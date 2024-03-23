package io.carml.engine.rdf.util;

import com.google.common.collect.Sets;
import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.rdf.MappedStatement;
import io.carml.model.Target;
import io.carml.util.ModelsException;
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

    @SafeVarargs
    public static Stream<MappingResult<Statement>> streamCartesianProductMappedStatements(
            Set<MappedValue<Resource>> mappedSubjects,
            Set<MappedValue<IRI>> mappedPredicates,
            Set<MappedValue<? extends Value>> mappedObjects,
            Set<MappedValue<Resource>> mappedGraphs,
            UnaryOperator<Resource> graphModifier,
            ValueFactory valueFactory,
            Consumer<Statement>... statementConsumers) {
        if (mappedSubjects.isEmpty() || mappedPredicates.isEmpty() || mappedObjects.isEmpty()) {
            throw new ModelsException("Could not create cartesian product statements because at least one of subjects,"
                    + " predicates or objects was empty.");
        }
        if (mappedGraphs.isEmpty()) {
            return Sets.cartesianProduct(mappedSubjects, mappedPredicates, mappedObjects).stream()
                    .map(element -> createMappedStatement(
                            element.get(0),
                            element.get(1),
                            element.get(2),
                            null,
                            graphModifier,
                            valueFactory,
                            statementConsumers));
        } else {
            return Sets.cartesianProduct(mappedSubjects, mappedPredicates, mappedObjects, mappedGraphs).stream()
                    .map(element -> createMappedStatement(
                            element.get(0),
                            element.get(1),
                            element.get(2),
                            element.get(3),
                            graphModifier,
                            valueFactory,
                            statementConsumers));
        }
    }

    @SafeVarargs
    public static Stream<MappingResult<Statement>> streamCartesianProductMappedStatementsForResourceObjects(
            Set<MappedValue<Resource>> mappedSubjects,
            Set<MappedValue<IRI>> mappedPredicates,
            Set<MappedValue<Resource>> mappedObjects,
            Set<MappedValue<Resource>> mappedGraphs,
            UnaryOperator<Resource> graphModifier,
            ValueFactory valueFactory,
            Consumer<Statement>... statementConsumers) {
        if (mappedSubjects.isEmpty() || mappedPredicates.isEmpty() || mappedObjects.isEmpty()) {
            throw new ModelsException("Could not create cartesian product statements because at least one of subjects,"
                    + " predicates or objects was empty.");
        }
        if (mappedGraphs.isEmpty()) {
            return Sets.cartesianProduct(mappedSubjects, mappedPredicates, mappedObjects).stream()
                    .map(element -> createMappedStatement(
                            element.get(0),
                            element.get(1),
                            element.get(2),
                            null,
                            graphModifier,
                            valueFactory,
                            statementConsumers));
        } else {
            return Sets.cartesianProduct(mappedSubjects, mappedPredicates, mappedObjects, mappedGraphs).stream()
                    .map(element -> createMappedStatement(
                            element.get(0),
                            element.get(1),
                            element.get(2),
                            element.get(3),
                            graphModifier,
                            valueFactory,
                            statementConsumers));
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

        var graphTargets = graphValue != null ? graphValue.getTargets() : Set.<Target>of();

        var targets = Stream.of(
                        subjectValue.getTargets(), predicateValue.getTargets(), objectValue.getTargets(), graphTargets)
                .flatMap(Set::stream)
                .collect(Collectors.toUnmodifiableSet());

        return MappedStatement.builder() //
                .statement(statement)
                .targets(targets)
                .build();
    }
}
