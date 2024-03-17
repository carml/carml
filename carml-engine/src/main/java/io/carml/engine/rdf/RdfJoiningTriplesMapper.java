package io.carml.engine.rdf;

import static io.carml.util.LogUtil.exception;

import io.carml.engine.TriplesMapper;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.model.LogicalSource;
import io.carml.model.PredicateObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfJoiningTriplesMapper<R> implements TriplesMapper<Statement> {

    @NonNull
    private final TriplesMap triplesMap;

    @NonNull
    private final LogicalSource virtualJoiningLogicalSource;

    private final Set<RdfSubjectMapper> subjectMappers;

    private final Set<RdfPredicateObjectMapper> predicateObjectMappers;

    @NonNull
    private final LogicalSourceResolver.ExpressionEvaluationFactory<R> expressionEvaluationFactory;

    private final LogicalSourceResolver.DatatypeMapperFactory<R> datatypeMapperFactory;

    @NonNull
    private final RdfMapperConfig rdfMapperConfig;

    public static <R> RdfJoiningTriplesMapper<R> of(
            @NonNull TriplesMap triplesMap,
            Set<PredicateObjectMap> joiningPredicateObjectMaps,
            LogicalSource virtualJoiningLogicalSource,
            @NonNull LogicalSourceResolver<R> logicalSourceResolver,
            @NonNull RdfMapperConfig rdfMapperConfig) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating mapper for Joining TriplesMap {}", triplesMap.getResourceName());
        }

        Set<RdfSubjectMapper> subjectMappers = createSubjectMappers(triplesMap, rdfMapperConfig);

        Set<RdfPredicateObjectMapper> predicateObjectMappers =
                createPredicateObjectMappers(triplesMap, joiningPredicateObjectMaps, rdfMapperConfig);

        return new RdfJoiningTriplesMapper<>(
                triplesMap,
                virtualJoiningLogicalSource,
                subjectMappers,
                predicateObjectMappers,
                logicalSourceResolver.getExpressionEvaluationFactory(),
                logicalSourceResolver.getDatatypeMapperFactory().orElse(null),
                rdfMapperConfig);
    }

    @SuppressWarnings("java:S3864")
    private static Set<RdfSubjectMapper> createSubjectMappers(TriplesMap triplesMap, RdfMapperConfig rdfMapperConfig) {

        Set<SubjectMap> subjectMaps = triplesMap.getSubjectMaps();
        if (subjectMaps.isEmpty()) {
            throw new TriplesMapperException(String.format(
                    "Subject map must be specified in triples map %s", exception(triplesMap, triplesMap)));
        }

        return subjectMaps.stream()
                .peek(sm -> LOG.debug("Creating mapper for SubjectMap {}", sm.getResourceName()))
                .map(sm -> RdfSubjectMapper.ofJoining(sm, triplesMap, rdfMapperConfig))
                .collect(Collectors.toUnmodifiableSet());
    }

    @SuppressWarnings("java:S3864")
    private static Set<RdfPredicateObjectMapper> createPredicateObjectMappers(
            TriplesMap triplesMap,
            Set<PredicateObjectMap> joiningPredicateObjectMaps,
            RdfMapperConfig rdfMapperConfig) {
        return joiningPredicateObjectMaps.stream()
                .peek(pom -> LOG.debug("Creating mapper for PredicateObjectMap {}", pom.getResourceName()))
                .map(pom -> RdfPredicateObjectMapper.forTableJoining(pom, triplesMap, rdfMapperConfig, "parent."))
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Flux<Statement> map(LogicalSourceRecord<?> logicalSourceRecord) {
        var sourceRecord = (R) logicalSourceRecord.getSourceRecord();
        LOG.trace("Mapping joining triples for record {}", logicalSourceRecord);
        var expressionEvaluation = expressionEvaluationFactory.apply(sourceRecord);
        var datatypeMapper = datatypeMapperFactory != null ? datatypeMapperFactory.apply(sourceRecord) : null;

        return mapEvaluation(expressionEvaluation, datatypeMapper);
    }

    @Override
    public Flux<Statement> mapEvaluation(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        Set<RdfSubjectMapper.Result> subjectMapperResults = subjectMappers.stream()
                .map(subjectMapper -> subjectMapper.map(expressionEvaluation, datatypeMapper))
                .collect(Collectors.toUnmodifiableSet());

        Set<Resource> subjects = subjectMapperResults.stream()
                .map(RdfSubjectMapper.Result::getSubjects)
                .flatMap(Set::stream)
                .collect(Collectors.toUnmodifiableSet());

        if (subjects.isEmpty()) {
            return Flux.empty();
        }

        Map<Set<Resource>, Set<Resource>> subjectsAndSubjectGraphs = new HashMap<>();
        List<Flux<Statement>> subjectStatementFluxes = new ArrayList<>();

        for (RdfSubjectMapper.Result subjectMapperResult : subjectMapperResults) {
            Set<Resource> resultSubjects = subjectMapperResult.getSubjects();
            if (!resultSubjects.isEmpty()) {
                subjectsAndSubjectGraphs.put(resultSubjects, subjectMapperResult.getGraphs());
                subjectStatementFluxes.add(subjectMapperResult.getTypeStatements());
            }
        }

        Flux<Statement> subjectStatements = Flux.merge(subjectStatementFluxes);
        Flux<Statement> pomStatements = Flux.fromIterable(predicateObjectMappers)
                .flatMap(predicateObjectMapper ->
                        predicateObjectMapper.map(expressionEvaluation, datatypeMapper, subjectsAndSubjectGraphs));

        return Flux.merge(subjectStatements, pomStatements);
    }

    @Override
    public TriplesMap getTriplesMap() {
        return triplesMap;
    }

    @Override
    public LogicalSource getLogicalSource() {
        return virtualJoiningLogicalSource;
    }

    @Override
    public ParentSideJoinConditionStore<Resource> getParentSideJoinConditions() {
        throw new UnsupportedOperationException("This method should never be called for this type.");
    }

    @Override
    public void cleanup() {
        // No cleanup necessary
    }
}
