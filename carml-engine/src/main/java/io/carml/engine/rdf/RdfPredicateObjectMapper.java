package io.carml.engine.rdf;

import static io.carml.engine.rdf.util.MappedStatements.streamCartesianProductMappedStatements;
import static io.carml.util.LogUtil.exception;
import static io.carml.util.LogUtil.log;
import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.BaseObjectMap;
import io.carml.model.LogicalSource;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfPredicateObjectMapper {

    private final Set<TermGenerator<Resource>> graphGenerators;

    private final Set<TermGenerator<IRI>> predicateGenerators;

    private final Set<TermGenerator<? extends Value>> objectGenerators;

    @Getter(AccessLevel.PACKAGE)
    private final Set<RdfRefObjectMapper> rdfRefObjectMappers;

    @NonNull
    private final ValueFactory valueFactory;

    public static RdfPredicateObjectMapper of(
            @NonNull PredicateObjectMap pom,
            @NonNull TriplesMap triplesMap,
            Set<RdfRefObjectMapper> refObjectMappers,
            @NonNull RdfMapperConfig rdfMapperConfig) {
        var rdfTermGeneratorFactory = (RdfTermGeneratorFactory) rdfMapperConfig.getTermGeneratorFactory();
        var graphGenerators = RdfTriplesMapper.createGraphGenerators(pom.getGraphMaps(), rdfTermGeneratorFactory);
        var predicateGenerators = createPredicateGenerators(pom, triplesMap, rdfTermGeneratorFactory);
        var objectMaps = pom.getObjectMaps();
        var objectGenerators = createObjectGenerators(objectMaps, triplesMap, rdfTermGeneratorFactory);

        var filteredRefObjectMappers = refObjectMappers.stream()
                .filter(rom -> pom.getObjectMaps().contains(rom.getRefObjectMap()))
                .collect(toUnmodifiableSet());

        return new RdfPredicateObjectMapper(
                graphGenerators,
                predicateGenerators,
                objectGenerators,
                filteredRefObjectMappers,
                rdfMapperConfig.getValueFactorySupplier().get());
    }

    public static RdfPredicateObjectMapper forTableJoining(
            @NonNull PredicateObjectMap pom,
            @NonNull TriplesMap triplesMap,
            @NonNull RdfMapperConfig rdfMapperConfig,
            String parentExpressionPrefix) {
        var rdfTermGeneratorFactory = (RdfTermGeneratorFactory) rdfMapperConfig.getTermGeneratorFactory();

        var graphGenerators = RdfTriplesMapper.createGraphGenerators(pom.getGraphMaps(), rdfTermGeneratorFactory);
        var predicateGenerators = createPredicateGenerators(pom, triplesMap, rdfTermGeneratorFactory);
        var objectMaps = pom.getObjectMaps();
        var objectGenerators = createTableJoiningRefObjectMapGenerators(
                objectMaps, triplesMap, rdfTermGeneratorFactory, parentExpressionPrefix);

        return new RdfPredicateObjectMapper(
                graphGenerators,
                predicateGenerators,
                objectGenerators,
                Set.of(),
                rdfMapperConfig.getValueFactorySupplier().get());
    }

    static Set<TermGenerator<IRI>> createPredicateGenerators(
            PredicateObjectMap pom, TriplesMap triplesMap, RdfTermGeneratorFactory termGeneratorFactory) {
        return pom.getPredicateMaps().stream()
                .map(predicateMap -> {
                    try {
                        return termGeneratorFactory.getPredicateGenerator(predicateMap);
                    } catch (RuntimeException ex) {
                        throw new TriplesMapperException(
                                String.format(
                                        "Exception occurred while creating predicate generator for %s",
                                        exception(triplesMap, predicateMap)),
                                ex);
                    }
                })
                .collect(toUnmodifiableSet());
    }

    private static Set<TermGenerator<? extends Value>> createObjectGenerators(
            Set<BaseObjectMap> objectMaps, TriplesMap triplesMap, RdfTermGeneratorFactory termGeneratorFactory) {
        return Stream.concat(
                        // object maps -> object generators
                        createObjectMapGenerators(objectMaps, triplesMap, termGeneratorFactory),
                        // ref object maps without joins -> object generators.
                        createJoinlessRefObjectMapGenerators(objectMaps, triplesMap, termGeneratorFactory))
                .collect(toUnmodifiableSet());
    }

    @SuppressWarnings("java:S3864")
    static Stream<TermGenerator<Value>> createObjectMapGenerators(
            Set<BaseObjectMap> objectMaps, TriplesMap triplesMap, RdfTermGeneratorFactory termGeneratorFactory) {
        return objectMaps.stream()
                .filter(ObjectMap.class::isInstance)
                .peek(objectMap -> LOG.debug("Creating term generator for ObjectMap {}", objectMap.getResourceName()))
                .map(objectMap -> {
                    try {
                        return termGeneratorFactory.getObjectGenerator((ObjectMap) objectMap);
                    } catch (RuntimeException ex) {
                        throw new TriplesMapperException(
                                String.format(
                                        "Exception occurred while creating object generator for %s",
                                        exception(triplesMap, objectMap)),
                                ex);
                    }
                });
    }

    private static RefObjectMap checkLogicalSource(
            RefObjectMap refObjectMap, LogicalSource logicalSource, TriplesMap triplesMap) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Checking if logicalSource for parent triples map {} is equal",
                    refObjectMap.getParentTriplesMap().getResourceName());
        }

        var parentLogicalSource =
                (LogicalSource) refObjectMap.getParentTriplesMap().getLogicalSource();

        if (parentLogicalSource == null) {
            throw new TriplesMapperException(String.format(
                    "Could not determine logical source of parent TriplesMap on RefObjectMap %s%nPossibly the parent "
                            + "triples map does not exist, or the reference to it is misspelled?",
                    exception(triplesMap, refObjectMap)));
        }

        if (!logicalSource.equals(parentLogicalSource)) {
            throw new TriplesMapperException(String.format(
                    "Logical sources are not equal.%n%nParent logical source: %s%n%nChild logical source: %s%n%nNot "
                            + "equal in RefObjectMap %s",
                    log(refObjectMap.getParentTriplesMap(), parentLogicalSource),
                    log(triplesMap, logicalSource),
                    exception(triplesMap, refObjectMap)));
        }

        return refObjectMap;
    }

    @SuppressWarnings("java:S3864")
    private static Stream<TermGenerator<? extends Value>> createJoinlessRefObjectMapGenerators(
            Set<BaseObjectMap> objectMaps, TriplesMap triplesMap, RdfTermGeneratorFactory termGeneratorFactory) {

        var logicalSource = (LogicalSource) triplesMap.getLogicalSource();

        return objectMaps.stream()
                .filter(RefObjectMap.class::isInstance)
                .peek(objectMap -> LOG.debug("Creating mapper for RefObjectMap {}", objectMap.getResourceName()))
                .map(RefObjectMap.class::cast)
                .filter(refObjMap -> refObjMap.getJoinConditions().isEmpty() || refObjMap.isSelfJoining(triplesMap))
                // ref object maps without joins MUST have an identical logical source.
                .map(refObjMap -> checkLogicalSource(refObjMap, logicalSource, triplesMap))
                .flatMap(refObjMap -> createRefObjectJoinlessMapper(refObjMap, triplesMap, termGeneratorFactory, null));
    }

    @SuppressWarnings("java:S3864")
    private static Set<TermGenerator<? extends Value>> createTableJoiningRefObjectMapGenerators(
            Set<BaseObjectMap> objectMaps,
            TriplesMap triplesMap,
            RdfTermGeneratorFactory termGeneratorFactory,
            String expressionPrefix) {

        return objectMaps.stream()
                .filter(RefObjectMap.class::isInstance)
                .peek(objectMap -> LOG.debug("Creating mapper for RefObjectMap {}", objectMap.getResourceName()))
                .map(RefObjectMap.class::cast)
                .flatMap(refObjMap ->
                        createRefObjectJoinlessMapper(refObjMap, triplesMap, termGeneratorFactory, expressionPrefix))
                .collect(toUnmodifiableSet());
    }

    private static Stream<TermGenerator<? extends Value>> createRefObjectJoinlessMapper(
            RefObjectMap refObjectMap,
            TriplesMap triplesMap,
            RdfTermGeneratorFactory termGeneratorFactory,
            String expressionPrefix) {
        try {
            return refObjectMap.getParentTriplesMap().getSubjectMaps().stream()
                    .map(parentSubjectMap -> expressionPrefix != null
                            ? parentSubjectMap.applyExpressionAdapter(
                                    refExpression -> String.format("%s%s", expressionPrefix, refExpression))
                            : parentSubjectMap)
                    .map(termGeneratorFactory::getSubjectGenerator);
        } catch (RuntimeException ex) {
            throw new TriplesMapperException(
                    String.format("Exception occurred for %s", exception(triplesMap, refObjectMap)), ex);
        }
    }

    public Flux<MappingResult<Statement>> map(
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper,
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndSubjectGraphs) {
        Set<MappedValue<IRI>> predicates = predicateGenerators.stream()
                .map(g -> g.apply(expressionEvaluation, datatypeMapper))
                .flatMap(List::stream)
                .collect(toUnmodifiableSet());

        if (predicates.isEmpty()) {
            return Flux.empty();
        }

        var objects = objectGenerators.stream()
                .map(g -> g.apply(expressionEvaluation, datatypeMapper))
                .<MappedValue<? extends Value>>flatMap(List::stream)
                .toList();

        Set<MappedValue<Resource>> pomGraphs = graphGenerators.stream()
                .flatMap(graphGenerator -> graphGenerator.apply(expressionEvaluation, datatypeMapper).stream())
                .collect(toUnmodifiableSet());

        var subjectsAndAllGraphs = addPomGraphsToSubjectsAndSubjectGraphs(subjectsAndSubjectGraphs, pomGraphs);

        // process RefObjectMaps for resolving later
        rdfRefObjectMappers.forEach(
                rdfRefObjectMapper -> rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation));

        if (objects.isEmpty()) {
            return Flux.empty();
        }

        // Separate mergeable collection objects from regular objects.
        // Mergeables with graphs need special handling: they must not participate in the
        // cartesian product because each graph requires independent blank nodes and linking triples.
        var mergeableObjects = objects.stream()
                .filter(obj -> isMergeableCollection(obj) && !pomGraphs.isEmpty())
                .toList();

        var regularObjects =
                objects.stream().filter(obj -> !mergeableObjects.contains(obj)).toList();

        Set<Flux<MappingResult<Statement>>> statementsPerGraphSet = new HashSet<>();

        if (!regularObjects.isEmpty()) {
            subjectsAndAllGraphs.entrySet().stream()
                    .map(subjectsAndAllGraphsEntry -> Flux.fromStream(streamCartesianProductMappedStatements(
                            subjectsAndAllGraphsEntry.getKey(),
                            predicates,
                            regularObjects,
                            subjectsAndAllGraphsEntry.getValue(),
                            RdfTriplesMapper.defaultGraphModifier,
                            valueFactory,
                            RdfTriplesMapper.logAddStatements)))
                    .forEach(statementsPerGraphSet::add);
        }

        // For mergeable collection objects with graphs, create graph-scoped instances
        // that carry linking triple info (subjects, predicates) and will generate per-graph
        // structures with fresh blank nodes after merging.
        if (!mergeableObjects.isEmpty()) {
            var mergeableCollectionResults = Flux.fromStream(mergeableObjects.stream()
                    .flatMap(obj -> scopeMergeableForGraphs(obj, subjectsAndAllGraphs, predicates)));

            statementsPerGraphSet.add(mergeableCollectionResults);
        }

        var collectionResults = Flux.fromStream(
                Stream.<Collection<? extends MappedValue<? extends Value>>>of(predicates, regularObjects, pomGraphs)
                        .flatMap(Collection::stream)
                        .map(mappedValue -> getCollectionResults(mappedValue, pomGraphs))
                        .filter(Objects::nonNull));

        statementsPerGraphSet.add(collectionResults);

        return Flux.merge(statementsPerGraphSet);
    }

    private static boolean isMergeableCollection(MappedValue<? extends Value> mappedValue) {
        return mappedValue instanceof MergeableRdfList || mappedValue instanceof MergeableRdfContainer;
    }

    /**
     * Creates graph-scoped mergeable collection instances that carry linking triple info.
     * The linking triples (subject-predicate-head-graph) will be generated by the mergeable
     * after merging, when {@code getResults()} is called.
     */
    @SuppressWarnings("unchecked")
    private Stream<MappingResult<Statement>> scopeMergeableForGraphs(
            MappedValue<? extends Value> mergeableObj,
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndAllGraphs,
            Set<MappedValue<IRI>> predicates) {
        var predicateIris = predicates.stream().map(MappedValue::getValue).collect(toUnmodifiableSet());

        return subjectsAndAllGraphs.entrySet().stream().flatMap(entry -> {
            var subjectResources =
                    entry.getKey().stream().map(MappedValue::getValue).collect(toUnmodifiableSet());

            var graphResources = entry.getValue().stream()
                    .map(mv -> RdfTriplesMapper.defaultGraphModifier.apply(mv.getValue()))
                    .filter(Objects::nonNull)
                    .collect(toUnmodifiableSet());

            if (graphResources.isEmpty()) {
                // No effective graphs; fall back to default graph behavior
                return Stream.of((MappingResult<Statement>) mergeableObj);
            }

            if (mergeableObj instanceof MergeableRdfList<?> mergeableList) {
                return Stream.of((MappingResult<Statement>)
                        mergeableList.withGraphScope(graphResources, subjectResources, predicateIris));
            } else if (mergeableObj instanceof MergeableRdfContainer<?> mergeableContainer) {
                return Stream.of((MappingResult<Statement>)
                        mergeableContainer.withGraphScope(graphResources, subjectResources, predicateIris));
            }

            return Stream.empty();
        });
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

    private Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> addPomGraphsToSubjectsAndSubjectGraphs(
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndSubjectGraphs,
            Set<MappedValue<Resource>> pomGraphs) {
        return subjectsAndSubjectGraphs.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, subjectsAndSubjectGraphsEntry -> Stream.concat(
                                subjectsAndSubjectGraphsEntry.getValue().stream(), pomGraphs.stream())
                        .collect(toUnmodifiableSet())));
    }
}
