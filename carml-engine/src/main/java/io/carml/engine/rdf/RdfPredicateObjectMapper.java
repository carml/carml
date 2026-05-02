package io.carml.engine.rdf;

import static io.carml.engine.rdf.util.MappedStatements.streamCartesianProductBytes;
import static io.carml.engine.rdf.util.MappedStatements.streamCartesianProductMappedStatements;
import static io.carml.util.LogUtil.exception;
import static io.carml.util.LogUtil.log;
import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.StreamingTermGenerator;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.rdf.cc.MergeableRdfContainer;
import io.carml.engine.rdf.cc.MergeableRdfList;
import io.carml.engine.rdf.cc.RdfContainer;
import io.carml.engine.rdf.cc.RdfList;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.BaseObjectMap;
import io.carml.model.LogicalSource;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import io.carml.output.NTriplesTermEncoder;
import java.util.ArrayList;
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

    @NonNull
    private final ValueFactory valueFactory;

    public static RdfPredicateObjectMapper of(
            @NonNull PredicateObjectMap pom, @NonNull TriplesMap triplesMap, @NonNull RdfMapperConfig rdfMapperConfig) {
        var rdfTermGeneratorFactory = (RdfTermGeneratorFactory) rdfMapperConfig.getTermGeneratorFactory();
        var graphGenerators = RdfTriplesMapper.createGraphGenerators(pom.getGraphMaps(), rdfTermGeneratorFactory);
        var predicateGenerators = createPredicateGenerators(pom, triplesMap, rdfTermGeneratorFactory);
        var objectMaps = pom.getObjectMaps();
        var objectGenerators = createObjectGenerators(objectMaps, triplesMap, rdfTermGeneratorFactory);

        return new RdfPredicateObjectMapper(
                graphGenerators,
                predicateGenerators,
                objectGenerators,
                rdfMapperConfig.getValueFactorySupplier().get());
    }

    /**
     * Creates an {@link RdfPredicateObjectMapper} for the unified view path. Joinless RefObjectMaps
     * are handled inline (same as the LS path), while joined RefObjectMaps use the expression prefix
     * from the view left joins.
     *
     * @param pom the predicate-object map
     * @param triplesMap the TriplesMap containing the POM
     * @param rdfMapperConfig the mapper configuration
     * @param rdfTermGeneratorFactory the term generator factory (may include refObjectMapPrefixes)
     * @param refObjectMapPrefixes mapping from each handled RefObjectMap to its expression prefix
     * @return a new {@link RdfPredicateObjectMapper} for the view path
     */
    public static RdfPredicateObjectMapper forView(
            PredicateObjectMap pom,
            TriplesMap triplesMap,
            RdfMapperConfig rdfMapperConfig,
            RdfTermGeneratorFactory rdfTermGeneratorFactory,
            Map<RefObjectMap, String> refObjectMapPrefixes) {

        var graphGenerators = RdfTriplesMapper.createGraphGenerators(pom.getGraphMaps(), rdfTermGeneratorFactory);
        var predicateGenerators = createPredicateGenerators(pom, triplesMap, rdfTermGeneratorFactory);
        var objectMaps = pom.getObjectMaps();

        Set<TermGenerator<? extends Value>> objectGenerators = Stream.concat(
                        Stream.concat(
                                createObjectMapGenerators(objectMaps, triplesMap, rdfTermGeneratorFactory),
                                createJoinlessRefObjectMapGeneratorsForView(
                                        objectMaps, triplesMap, rdfTermGeneratorFactory)),
                        createJoinedRefObjectMapGenerators(
                                objectMaps, triplesMap, rdfTermGeneratorFactory, refObjectMapPrefixes))
                .collect(toUnmodifiableSet());

        return new RdfPredicateObjectMapper(
                graphGenerators,
                predicateGenerators,
                objectGenerators,
                rdfMapperConfig.getValueFactorySupplier().get());
    }

    /**
     * Creates term generators for joinless RefObjectMaps in the view path. Works when the TriplesMap
     * logicalSource is still a LogicalSource (pre-wrapping).
     */
    private static Stream<TermGenerator<? extends Value>> createJoinlessRefObjectMapGeneratorsForView(
            Set<BaseObjectMap> objectMaps, TriplesMap triplesMap, RdfTermGeneratorFactory termGeneratorFactory) {
        var logicalSource = triplesMap.getLogicalSource();
        if (!(logicalSource instanceof LogicalSource ls)) {
            return Stream.empty();
        }
        return objectMaps.stream()
                .filter(RefObjectMap.class::isInstance)
                .map(RefObjectMap.class::cast)
                .filter(rom -> rom.getJoinConditions().isEmpty() || rom.isSelfJoining(triplesMap))
                .map(rom -> checkLogicalSource(rom, ls, triplesMap))
                .flatMap(rom -> createRefObjectJoinlessMapper(rom, triplesMap, termGeneratorFactory, null));
    }

    /**
     * Creates term generators for joined RefObjectMaps using the expression prefix from view left
     * joins.
     */
    private static Stream<TermGenerator<? extends Value>> createJoinedRefObjectMapGenerators(
            Set<BaseObjectMap> objectMaps,
            TriplesMap triplesMap,
            RdfTermGeneratorFactory termGeneratorFactory,
            Map<RefObjectMap, String> refObjectMapPrefixes) {
        return objectMaps.stream()
                .filter(RefObjectMap.class::isInstance)
                .map(RefObjectMap.class::cast)
                .filter(rom -> !rom.getJoinConditions().isEmpty() && !rom.isSelfJoining(triplesMap))
                .filter(refObjectMapPrefixes::containsKey)
                .flatMap(rom -> createRefObjectJoinlessMapper(
                        rom, triplesMap, termGeneratorFactory, refObjectMapPrefixes.get(rom)));
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

        // LogicalView-based TMs do not support joinless RefObjectMaps — joins are handled by the
        // LogicalView evaluator. Skip the cast and return an empty stream.
        if (!(triplesMap.getLogicalSource() instanceof LogicalSource logicalSource)) {
            return Stream.empty();
        }

        return objectMaps.stream()
                .filter(RefObjectMap.class::isInstance)
                .peek(objectMap -> LOG.debug("Creating mapper for RefObjectMap {}", objectMap.getResourceName()))
                .map(RefObjectMap.class::cast)
                .filter(refObjMap -> refObjMap.getJoinConditions().isEmpty() || refObjMap.isSelfJoining(triplesMap))
                // ref object maps without joins MUST have an identical logical source.
                .map(refObjMap -> checkLogicalSource(refObjMap, logicalSource, triplesMap))
                .flatMap(refObjMap -> createRefObjectJoinlessMapper(refObjMap, triplesMap, termGeneratorFactory, null));
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
        var pom = evaluatePom(expressionEvaluation, datatypeMapper, subjectsAndSubjectGraphs);
        if (pom == null) {
            return Flux.empty();
        }

        Set<Flux<MappingResult<Statement>>> statementsPerGraphSet = new HashSet<>();

        if (!pom.regularObjects().isEmpty()) {
            pom.subjectsAndAllGraphs().entrySet().stream()
                    .map(entry -> Flux.fromStream(streamCartesianProductMappedStatements(
                            entry.getKey(),
                            pom.predicates(),
                            pom.regularObjects(),
                            entry.getValue(),
                            RdfTriplesMapper.defaultGraphModifier,
                            valueFactory,
                            RdfTriplesMapper.logAddStatements)))
                    .forEach(statementsPerGraphSet::add);
        }

        // For mergeable collection objects with graphs, create graph-scoped instances
        // that carry linking triple info (subjects, predicates) and will generate per-graph
        // structures with fresh blank nodes after merging.
        if (!pom.mergeableObjects().isEmpty()) {
            var mergeableCollectionResults = Flux.fromStream(pom.mergeableObjects().stream()
                    .flatMap(obj -> scopeMergeableForGraphs(obj, pom.subjectsAndAllGraphs(), pom.predicates())));

            statementsPerGraphSet.add(mergeableCollectionResults);
        }

        var collectionResults = Flux.fromStream(Stream.<Collection<? extends MappedValue<? extends Value>>>of(
                        pom.predicates(), pom.regularObjects(), pom.pomGraphs())
                .flatMap(Collection::stream)
                .map(mappedValue -> getCollectionResults(mappedValue, pom.pomGraphs()))
                .filter(Objects::nonNull));

        statementsPerGraphSet.add(collectionResults);

        // Streaming generators: build a per-generator flux that emits link triples + per-object
        // collection results lazily. Subscribing only allocates a working-set window of one rdf:List /
        // rdf:Container at a time, regardless of total cartesian-product cardinality.
        for (var streamingGen : pom.streamingGenerators()) {
            var streamingFlux = streamingObjectsFlux(
                    streamingGen,
                    expressionEvaluation,
                    datatypeMapper,
                    pom.subjectsAndAllGraphs(),
                    pom.predicates(),
                    pom.pomGraphs());
            statementsPerGraphSet.add(streamingFlux);
        }

        return Flux.merge(statementsPerGraphSet);
    }

    /**
     * Evaluates the per-iteration generators and partitions object generators into the eager and
     * streaming buckets shared by {@link #map} and {@link #mapToBytes}. Returns {@code null} when
     * the POM produces no output for this iteration (no predicates, or no objects from either
     * bucket) — callers translate that into {@code Flux.empty()}.
     */
    private EvaluatedPom evaluatePom(
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper,
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndSubjectGraphs) {
        Set<MappedValue<IRI>> predicates = predicateGenerators.stream()
                .map(g -> g.apply(expressionEvaluation, datatypeMapper))
                .flatMap(List::stream)
                .collect(toUnmodifiableSet());

        if (predicates.isEmpty()) {
            return null;
        }

        // Partition object generators: streaming generators emit values lazily on subscription so the
        // per-row cartesian-product cardinality is bounded by the working-set size, not the full
        // product. Eager generators continue to materialize their results once per row.
        List<StreamingTermGenerator<? extends Value>> streamingGenerators = objectGenerators.stream()
                .filter(StreamingTermGenerator.class::isInstance)
                .<StreamingTermGenerator<? extends Value>>map(g -> (StreamingTermGenerator<? extends Value>) g)
                .toList();

        var eagerObjects = objectGenerators.stream()
                .filter(g -> !(g instanceof StreamingTermGenerator))
                .map(g -> g.apply(expressionEvaluation, datatypeMapper))
                .<MappedValue<? extends Value>>flatMap(List::stream)
                .toList();

        if (eagerObjects.isEmpty() && streamingGenerators.isEmpty()) {
            return null;
        }

        Set<MappedValue<Resource>> pomGraphs = graphGenerators.stream()
                .flatMap(graphGenerator -> graphGenerator.apply(expressionEvaluation, datatypeMapper).stream())
                .collect(toUnmodifiableSet());

        var subjectsAndAllGraphs = addPomGraphsToSubjectsAndSubjectGraphs(subjectsAndSubjectGraphs, pomGraphs);

        // Mergeables-with-graphs go through the cross-iteration accumulator path. Everything else
        // (including non-mergeable RdfList/RdfContainer instances) flows through the cartesian-product
        // step so the linking triple <subject, predicate, head> is emitted alongside the structural
        // triples emitted from the collection-results stream.
        var mergeableObjects = eagerObjects.stream()
                .filter(obj -> isMergeableCollection(obj) && !pomGraphs.isEmpty())
                .toList();

        var regularObjects = eagerObjects.stream()
                .filter(obj -> !mergeableObjects.contains(obj))
                .toList();

        return new EvaluatedPom(
                predicates, streamingGenerators, regularObjects, mergeableObjects, pomGraphs, subjectsAndAllGraphs);
    }

    /**
     * Per-iteration POM evaluation snapshot shared by the statement and byte emission paths.
     * {@code regularObjects} are the eager objects that flow through the cartesian-product step;
     * {@code mergeableObjects} are eager mergeable collections with graphs that need cross-iteration
     * accumulation; {@code streamingGenerators} hold their per-row output until subscription.
     */
    private record EvaluatedPom(
            Set<MappedValue<IRI>> predicates,
            List<StreamingTermGenerator<? extends Value>> streamingGenerators,
            List<MappedValue<? extends Value>> regularObjects,
            List<MappedValue<? extends Value>> mergeableObjects,
            Set<MappedValue<Resource>> pomGraphs,
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndAllGraphs) {}

    /**
     * Builds a backpressure-aware {@link Flux} for a single streaming object generator. Each emitted
     * object is fanned out into its own link-triple stream (subject × predicate × object × graph) and
     * the collection-results stream for the object's structural triples; the generator stream is
     * consumed lazily so peak working-set is one collection at a time.
     */
    private Flux<MappingResult<Statement>> streamingObjectsFlux(
            StreamingTermGenerator<? extends Value> streamingGen,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper,
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndAllGraphs,
            Set<MappedValue<IRI>> predicates,
            Set<MappedValue<Resource>> pomGraphs) {
        return Flux.<MappedValue<? extends Value>>fromStream(
                        () -> streamingGen.applyAsStream(expressionEvaluation, datatypeMapper))
                .concatMap(obj -> {
                    if (isMergeableCollection(obj) && !pomGraphs.isEmpty()) {
                        // Mergeable streaming objects are not produced by the cartesian-product gather
                        // strategy in scope here. If a future generator violates this assumption, the
                        // streaming path would be unsafe (mergeables require cross-iteration accumulation
                        // and graph-scoped instance creation, neither of which is wired up here).
                        return Flux.error(new IllegalStateException(
                                "Streaming term generator emitted a mergeable collection object with graphs. "
                                        + "Mergeable collections must be handled via the eager accumulator path."));
                    }
                    var perObjectFluxes = subjectsAndAllGraphs.entrySet().stream()
                            .map(entry -> Flux.fromStream(() -> streamCartesianProductMappedStatements(
                                    entry.getKey(),
                                    predicates,
                                    List.of(obj),
                                    entry.getValue(),
                                    RdfTriplesMapper.defaultGraphModifier,
                                    valueFactory,
                                    RdfTriplesMapper.logAddStatements)))
                            .toList();

                    var linkTriples = Flux.concat(perObjectFluxes);

                    var collResult = getCollectionResults(obj, pomGraphs);
                    if (collResult == null) {
                        return linkTriples;
                    }
                    return linkTriples.concatWith(Flux.just(collResult));
                });
    }

    /**
     * Encodes predicate-object mapping results directly to N-Triples/N-Quads bytes, bypassing
     * {@link Statement} creation. Regular objects are encoded via the cartesian product byte path.
     * Collection objects ({@link RdfList}, {@link RdfContainer}) and mergeable collections have their
     * internal Statements encoded to bytes via the encoder.
     *
     * <p>Mergeable collection objects are returned separately via the provided accumulator list so
     * the caller can handle cross-iteration merging.
     *
     * <p>Returns a {@link Flux} of byte arrays. Streaming object generators (e.g. cartesian-product
     * gather maps) emit their outputs lazily — the caller subscribes once and the per-object link
     * triples and structural triples flow through without materializing the full cartesian product.
     *
     * @param expressionEvaluation the expression evaluation for the current iteration
     * @param datatypeMapper the datatype mapper for the current iteration
     * @param subjectsAndSubjectGraphs subjects mapped to their associated graphs
     * @param encoder the encoder to use for byte serialization
     * @param mergeableAccumulator accumulator for mergeable results that must be handled by the caller
     * @param includeGraph whether to include the graph field in encoded output (true for N-Quads,
     *     false for N-Triples)
     * @return a {@link Flux} of encoded byte arrays
     */
    Flux<byte[]> mapToBytes(
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper,
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndSubjectGraphs,
            NTriplesTermEncoder encoder,
            List<MappingResult<Statement>> mergeableAccumulator,
            boolean includeGraph) {
        var pom = evaluatePom(expressionEvaluation, datatypeMapper, subjectsAndSubjectGraphs);
        if (pom == null) {
            return Flux.empty();
        }

        var fluxes = new ArrayList<Flux<byte[]>>();

        if (!pom.regularObjects().isEmpty()) {
            // Encode regular objects via cartesian product bytes. This is also where non-mergeable
            // RdfList/RdfContainer linking triples are emitted: their getValue() returns the head/
            // container BNode, so the cartesian product yields <s, p, head> naturally.
            for (var entry : pom.subjectsAndAllGraphs().entrySet()) {
                var subjects = entry.getKey();
                var graphs = entry.getValue();
                fluxes.add(Flux.fromStream(() -> streamCartesianProductBytes(
                        subjects,
                        pom.predicates(),
                        pom.regularObjects(),
                        graphs,
                        RdfTriplesMapper.defaultGraphModifier,
                        encoder,
                        includeGraph)));
            }
        }

        // Encode collection results discovered in any of (predicates, regularObjects, pomGraphs).
        // For RdfList/RdfContainer objects in regularObjects this yields the structural triples
        // (rdf:type + rdf:_<n> members for containers, or the rdf:first/rdf:rest cons cells for
        // lists). Flux.from(coll.getResults()) is safe: getResults() builds statements lazily via a
        // synchronous Stream (no scheduler-crossing).
        var collectionFluxes = Stream.<Collection<? extends MappedValue<? extends Value>>>of(
                        pom.predicates(), pom.regularObjects(), pom.pomGraphs())
                .flatMap(Collection::stream)
                .map(mappedValue -> getCollectionResults(mappedValue, pom.pomGraphs()))
                .filter(Objects::nonNull)
                .map(collResult ->
                        Flux.from(collResult.getResults()).map(stmt -> encodeStatement(stmt, encoder, includeGraph)))
                .toList();
        fluxes.addAll(collectionFluxes);

        // Handle mergeable objects -- collect them for cross-iteration merging by the caller.
        // Mergeables resolve to byte output later when the merged tail is emitted; the caller writes
        // them into mergeableAccumulator synchronously before subscribing to the returned flux so the
        // accumulator is populated by the time downstream observers inspect it.
        if (!pom.mergeableObjects().isEmpty()) {
            pom.mergeableObjects().stream()
                    .flatMap(obj -> scopeMergeableForGraphs(obj, pom.subjectsAndAllGraphs(), pom.predicates()))
                    .forEach(mergeableAccumulator::add);
        }

        // Streaming generators: build a per-generator flux that emits link triples + per-object
        // structural triples directly as bytes, with no intermediate List<byte[]>.
        for (var streamingGen : pom.streamingGenerators()) {
            fluxes.add(streamingObjectsBytesFlux(
                    streamingGen,
                    expressionEvaluation,
                    datatypeMapper,
                    pom.subjectsAndAllGraphs(),
                    pom.predicates(),
                    pom.pomGraphs(),
                    encoder,
                    includeGraph));
        }

        if (fluxes.isEmpty()) {
            return Flux.empty();
        }

        return Flux.concat(fluxes);
    }

    /**
     * Builds a {@link Flux} of encoded bytes for one streaming object generator. Per-object link
     * triples and structural triples are emitted lazily via {@code concatMap} so peak working-set
     * stays bounded in the per-record cartesian-product cardinality.
     */
    private Flux<byte[]> streamingObjectsBytesFlux(
            StreamingTermGenerator<? extends Value> streamingGen,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper,
            Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndAllGraphs,
            Set<MappedValue<IRI>> predicates,
            Set<MappedValue<Resource>> pomGraphs,
            NTriplesTermEncoder encoder,
            boolean includeGraph) {
        return Flux.<MappedValue<? extends Value>>fromStream(
                        () -> streamingGen.applyAsStream(expressionEvaluation, datatypeMapper))
                .concatMap(obj -> {
                    if (isMergeableCollection(obj) && !pomGraphs.isEmpty()) {
                        return Flux.error(new IllegalStateException(
                                "Streaming term generator emitted a mergeable collection object with graphs. "
                                        + "Mergeable collections must be handled via the eager accumulator path."));
                    }
                    var perObjectByteFluxes = subjectsAndAllGraphs.entrySet().stream()
                            .map(entry -> Flux.fromStream(() -> streamCartesianProductBytes(
                                    entry.getKey(),
                                    predicates,
                                    List.of(obj),
                                    entry.getValue(),
                                    RdfTriplesMapper.defaultGraphModifier,
                                    encoder,
                                    includeGraph)))
                            .toList();

                    var linkBytes = Flux.concat(perObjectByteFluxes);

                    var collResult = getCollectionResults(obj, pomGraphs);
                    if (collResult == null) {
                        return linkBytes;
                    }
                    var collBytes = Flux.from(collResult.getResults())
                            .map(stmt -> encodeStatement(stmt, encoder, includeGraph));
                    return linkBytes.concatWith(collBytes);
                });
    }

    private static byte[] encodeStatement(Statement stmt, NTriplesTermEncoder encoder, boolean includeGraph) {
        if (includeGraph) {
            return encoder.encodeNQuad(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext());
        }
        return encoder.encodeNTriple(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
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
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        subjectsAndSubjectGraphsEntry -> Stream.concat(
                                        subjectsAndSubjectGraphsEntry.getValue().stream(), pomGraphs.stream())
                                .collect(toUnmodifiableSet())));
    }
}
