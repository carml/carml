package io.carml.engine.rdf;

import static io.carml.engine.rdf.util.MappedStatements.streamCartesianProductMappedStatements;
import static io.carml.util.LogUtil.exception;
import static io.carml.util.LogUtil.log;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfSubjectMapper {

    @NonNull
    private final TermGenerator<Resource> subjectGenerator;

    private final Set<TermGenerator<Resource>> graphGenerators;

    private final Set<MappedValue<? extends Value>> classes;

    @NonNull
    private final ValueFactory valueFactory;

    public static RdfSubjectMapper of(
            @NonNull SubjectMap subjectMap, @NonNull TriplesMap triplesMap, @NonNull RdfMapperConfig rdfMapperConfig) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating mapper for SubjectMap {}", log(triplesMap, subjectMap));
        }

        RdfTermGeneratorFactory rdfTermGeneratorFactory =
                (RdfTermGeneratorFactory) rdfMapperConfig.getTermGeneratorFactory();

        TermGenerator<Resource> subjectGenerator;
        try {
            subjectGenerator = rdfTermGeneratorFactory.getSubjectGenerator(subjectMap);
        } catch (RuntimeException ex) {
            throw new TriplesMapperException(
                    String.format(
                            "Exception occurred while creating subject generator for %s",
                            exception(triplesMap, subjectMap)),
                    ex);
        }

        Set<MappedValue<? extends Value>> classes = subjectMap //
                .getClasses()
                .stream()
                .map(RdfMappedValue::of)
                .collect(Collectors.toUnmodifiableSet());

        return new RdfSubjectMapper(
                subjectGenerator,
                RdfTriplesMapper.createGraphGenerators(subjectMap.getGraphMaps(), rdfTermGeneratorFactory),
                classes,
                rdfMapperConfig.getValueFactorySupplier().get());
    }

    public static RdfSubjectMapper ofJoining(
            @NonNull SubjectMap subjectMap, @NonNull TriplesMap triplesMap, @NonNull RdfMapperConfig rdfMapperConfig) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating mapper for SubjectMap {}", log(triplesMap, subjectMap));
        }

        RdfTermGeneratorFactory rdfTermGeneratorFactory =
                (RdfTermGeneratorFactory) rdfMapperConfig.getTermGeneratorFactory();

        TermGenerator<Resource> subjectGenerator;
        try {
            subjectGenerator = rdfTermGeneratorFactory.getSubjectGenerator(subjectMap);
        } catch (RuntimeException ex) {
            throw new TriplesMapperException(
                    String.format(
                            "Exception occurred while creating subject generator for %s",
                            exception(triplesMap, subjectMap)),
                    ex);
        }

        return new RdfSubjectMapper(
                subjectGenerator,
                RdfTriplesMapper.createGraphGenerators(subjectMap.getGraphMaps(), rdfTermGeneratorFactory),
                Set.of(),
                rdfMapperConfig.getValueFactorySupplier().get());
    }

    public Result map(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        LOG.debug("Determining subjects ...");

        // graphs to be used when generating statements in predicate object mapper
        Set<MappedValue<Resource>> graphs = graphGenerators.stream()
                .flatMap(graph -> graph.apply(expressionEvaluation, datatypeMapper).stream())
                .collect(Collectors.toUnmodifiableSet());

        var subjects = Set.copyOf(subjectGenerator.apply(expressionEvaluation, datatypeMapper));

        LOG.debug("Determined subjects {}", subjects);

        if (subjects.isEmpty()) {
            return resultOf(subjects, Set.of(), Flux.empty());
        }

        Flux<MappingResult<Statement>> typeStatements =
                classes.isEmpty() ? Flux.empty() : mapTypeStatements(subjects, graphs);

        var collectionResults = Flux.fromStream(Stream.concat(subjects.stream(), graphs.stream())
                .map(result -> getCollectionResults(result, graphs))
                .filter(Objects::nonNull));

        return resultOf(subjects, graphs, Flux.merge(typeStatements, collectionResults));
    }

    private MappingResult<Statement> getCollectionResults(
            MappedValue<? extends Value> mappedValue, Set<MappedValue<Resource>> graphs) {
        if (mappedValue instanceof RdfList<? extends Value> rdfList) {
            return graphs.isEmpty() ? rdfList : rdfList.withGraphs(graphs);
        } else if (mappedValue instanceof RdfContainer<? extends Value> rdfContainer) {
            return graphs.isEmpty() ? rdfContainer : rdfContainer.withGraphs(graphs);
        } else {
            return null;
        }
    }

    private Flux<MappingResult<Statement>> mapTypeStatements(
            Set<MappedValue<Resource>> subjects, Set<MappedValue<Resource>> graphs) {
        LOG.debug(
                "Generating triples for subjects: {}",
                subjects.stream().map(MappedValue::getValue).toList());

        Stream<MappingResult<Statement>> typeStatementStream = streamCartesianProductMappedStatements(
                subjects,
                Set.of(RdfMappedValue.of(RDF.TYPE)),
                classes,
                graphs,
                RdfTriplesMapper.defaultGraphModifier,
                valueFactory,
                RdfTriplesMapper.logAddStatements);

        return Flux.fromStream(typeStatementStream);
    }

    private static Result resultOf(
            Set<MappedValue<Resource>> subjects,
            Set<MappedValue<Resource>> graphs,
            Flux<MappingResult<Statement>> typeStatements) {
        return new Result(subjects, graphs, typeStatements);
    }

    @Getter
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Result {
        private Set<MappedValue<Resource>> subjects;

        private Set<MappedValue<Resource>> graphs;

        private Flux<MappingResult<Statement>> typeStatements;
    }
}
