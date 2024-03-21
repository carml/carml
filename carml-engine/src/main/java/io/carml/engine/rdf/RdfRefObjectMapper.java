package io.carml.engine.rdf;

import static io.carml.engine.rdf.util.MappedStatements.streamCartesianProductMappedStatementsForResourceObjects;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.RefObjectMapper;
import io.carml.engine.TriplesMapper;
import io.carml.engine.join.ChildSideJoin;
import io.carml.engine.join.ChildSideJoinCondition;
import io.carml.engine.join.ChildSideJoinStore;
import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.engine.join.ParentSideJoinKey;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfRefObjectMapper implements RefObjectMapper<Statement> {

    @NonNull
    private final RefObjectMap refObjectMap;

    @NonNull
    private final TriplesMap triplesMap;

    private final ChildSideJoinStore<MappedValue<Resource>, MappedValue<IRI>> childSideJoinStore;

    @NonNull
    private final ValueFactory valueFactory;

    public static RdfRefObjectMapper of(
            @NonNull RefObjectMap refObjectMap,
            @NonNull TriplesMap triplesMap,
            @NonNull RdfMapperConfig rdfMapperConfig) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating mapper for RefObjectMap {}", refObjectMap.getResourceName());
        }

        return new RdfRefObjectMapper(
                refObjectMap,
                triplesMap,
                rdfMapperConfig.getChildSideJoinStoreProvider().createChildSideJoinStore(refObjectMap.getId()),
                rdfMapperConfig.getValueFactorySupplier().get());
    }

    @Override
    public TriplesMap getTriplesMap() {
        return triplesMap;
    }

    @Override
    public RefObjectMap getRefObjectMap() {
        return refObjectMap;
    }

    public void map(
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndAllGraphs,
            Set<MappedValue<IRI>> predicates,
            ExpressionEvaluation expressionEvaluation) {
        prepareChildSideJoins(subjectsAndAllGraphs, predicates, expressionEvaluation);
    }

    private void prepareChildSideJoins(
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndAllGraphs,
            Set<MappedValue<IRI>> predicates,
            ExpressionEvaluation expressionEvaluation) {
        Set<ChildSideJoinCondition> childSideJoinConditions = refObjectMap.getJoinConditions().stream()
                .map(joinCondition -> {
                    // TODO
                    String childReference = joinCondition.getChildMap().getReference();

                    var childValues = expressionEvaluation
                            .apply(childReference)
                            .map(expressionResult ->
                                    new ArrayList<>(ExpressionEvaluation.extractStringValues(expressionResult)))
                            .orElse(new ArrayList<>());

                    return ChildSideJoinCondition.of(
                            childReference,
                            childValues,
                            joinCondition.getParentMap().getReference());
                })
                .collect(Collectors.toSet());

        Set<ChildSideJoin<MappedValue<Resource>, MappedValue<IRI>>> childSideJoins =
                subjectsAndAllGraphs.entrySet().stream()
                        .map(subjectsAndAllGraphsEntry -> prepareChildSideJoin(
                                subjectsAndAllGraphsEntry.getKey(),
                                predicates,
                                subjectsAndAllGraphsEntry.getValue(),
                                childSideJoinConditions))
                        .collect(Collectors.toUnmodifiableSet());

        childSideJoinStore.addAll(childSideJoins);
    }

    private ChildSideJoin<MappedValue<Resource>, MappedValue<IRI>> prepareChildSideJoin(
            Set<MappedValue<Resource>> subjects,
            Set<MappedValue<IRI>> predicates,
            Set<MappedValue<Resource>> graphs,
            Set<ChildSideJoinCondition> childSideJoinConditions) {
        return ChildSideJoin.<MappedValue<Resource>, MappedValue<IRI>>builder()
                .subjects(new HashSet<>(subjects))
                .predicates(new HashSet<>(predicates))
                .graphs(new HashSet<>(graphs))
                .childSideJoinConditions(new HashSet<>(childSideJoinConditions))
                .build();
    }

    @Override
    public Flux<MappingResult<Statement>> resolveJoins(TriplesMapper<Statement> parentTriplesMapper) {
        return childSideJoinStore
                .clearingFlux()
                .flatMap(childSideJoin -> resolveJoin(parentTriplesMapper, childSideJoin));
    }

    private Flux<MappingResult<Statement>> resolveJoin(
            TriplesMapper<Statement> parentTriplesMapper,
            ChildSideJoin<MappedValue<Resource>, MappedValue<IRI>> childSideJoin) {
        ParentSideJoinConditionStore<MappedValue<Resource>> parentJoinConditions =
                parentTriplesMapper.getParentSideJoinConditions();

        Set<MappedValue<Resource>> objects = checkJoinAndGetObjects(childSideJoin, parentJoinConditions);

        if (!objects.isEmpty()) {
            var subjects = childSideJoin.getSubjects();
            var predicates = childSideJoin.getPredicates();
            var graphs = childSideJoin.getGraphs();

            return Flux.fromStream(streamCartesianProductMappedStatementsForResourceObjects(
                    subjects,
                    predicates,
                    objects,
                    graphs,
                    RdfTriplesMapper.defaultGraphModifier,
                    valueFactory,
                    RdfTriplesMapper.logAddStatements));
        }

        return Flux.empty();
    }

    private Set<MappedValue<Resource>> checkJoinAndGetObjects(
            ChildSideJoin<MappedValue<Resource>, MappedValue<IRI>> childSideJoin,
            ParentSideJoinConditionStore<MappedValue<Resource>> parentJoinConditions) {

        List<Set<MappedValue<Resource>>> parentResults = childSideJoin.getChildSideJoinConditions().stream()
                .map(childSideJoinCondition ->
                        checkChildSideJoinCondition(childSideJoinCondition, parentJoinConditions))
                .toList();

        if (parentResults.isEmpty()) {
            return Set.of();
        } else if (parentResults.size() == 1) {
            return parentResults.get(0);
        }

        // return intersection of parentResults
        return parentResults.stream()
                .skip(1)
                .collect(() -> new HashSet<>(parentResults.get(0)), Set::retainAll, Set::retainAll);
    }

    private Set<MappedValue<Resource>> checkChildSideJoinCondition(
            ChildSideJoinCondition childSideJoinCondition,
            ParentSideJoinConditionStore<MappedValue<Resource>> parentJoinConditions) {

        return childSideJoinCondition.getChildValues().stream()
                .flatMap(childValue ->
                        checkChildSideJoinConditionChildValue(childSideJoinCondition, childValue, parentJoinConditions)
                                .stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    private Set<MappedValue<Resource>> checkChildSideJoinConditionChildValue(
            ChildSideJoinCondition childSideJoinCondition,
            String childValue,
            ParentSideJoinConditionStore<MappedValue<Resource>> parentJoinConditions) {
        ParentSideJoinKey parentSideKey = ParentSideJoinKey.of(childSideJoinCondition.getParentReference(), childValue);

        return parentJoinConditions.containsKey(parentSideKey) ? parentJoinConditions.get(parentSideKey) : Set.of();
    }
}
