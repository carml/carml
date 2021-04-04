package com.taxonic.carml.engine.rdf;

import static com.taxonic.carml.util.LogUtil.exception;
import static com.taxonic.carml.util.LogUtil.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.TriplesMapperException;
import com.taxonic.carml.model.BaseObjectMap;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.ModelUtil;
import java.util.List;
import java.util.Set;
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

  public static RdfPredicateObjectMapper of(@NonNull PredicateObjectMap pom, @NonNull TriplesMap triplesMap,
      Set<RdfRefObjectMapper> refObjectMappers, @NonNull RdfMappingContext rdfMappingContext) {

    RdfTermGeneratorFactory rdfTermGeneratorFactory =
        (RdfTermGeneratorFactory) rdfMappingContext.getTermGeneratorFactory();

    Set<TermGenerator<Resource>> graphGenerators =
        RdfTriplesMapper.createGraphGenerators(pom.getGraphMaps(), rdfTermGeneratorFactory);
    Set<TermGenerator<IRI>> predicateGenerators = createPredicateGenerators(pom, triplesMap, rdfTermGeneratorFactory);
    Set<BaseObjectMap> objectMaps = pom.getObjectMaps();
    Set<TermGenerator<? extends Value>> objectGenerators =
        createObjectGenerators(objectMaps, triplesMap, rdfTermGeneratorFactory);

    Set<RdfRefObjectMapper> filteredRefObjectMappers = refObjectMappers.stream()
        .filter(rom -> pom.getObjectMaps()
            .contains(rom.getRefObjectMap()))
        .collect(ImmutableSet.toImmutableSet());

    return new RdfPredicateObjectMapper(graphGenerators, predicateGenerators, objectGenerators,
        filteredRefObjectMappers, rdfMappingContext.getValueFactorySupplier()
            .get());
  }

  private static Set<TermGenerator<IRI>> createPredicateGenerators(PredicateObjectMap pom, TriplesMap triplesMap,
      RdfTermGeneratorFactory termGeneratorFactory) {
    return pom.getPredicateMaps()
        .stream()
        .map(predicateMap -> {
          try {
            return termGeneratorFactory.getPredicateGenerator(predicateMap);
          } catch (RuntimeException ex) {
            throw new TriplesMapperException(
                String.format("Exception occurred while creating predicate generator for %s",
                    exception(triplesMap, predicateMap)),
                ex);
          }
        })
        .collect(ImmutableSet.toImmutableSet());
  }

  private static Set<TermGenerator<? extends Value>> createObjectGenerators(Set<BaseObjectMap> objectMaps,
      TriplesMap triplesMap, RdfTermGeneratorFactory termGeneratorFactory) {
    return Stream.concat(
        // object maps -> object generators
        createObjectMapGenerators(objectMaps, triplesMap, termGeneratorFactory),
        // ref object maps without joins -> object generators.
        createJoinlessRefObjectMapGenerators(objectMaps, triplesMap, termGeneratorFactory))
        .collect(ImmutableSet.toImmutableSet());
  }

  private static Stream<TermGenerator<? extends Value>> createObjectMapGenerators(Set<BaseObjectMap> objectMaps,
      TriplesMap triplesMap, RdfTermGeneratorFactory termGeneratorFactory) {
    return objectMaps.stream()
        .filter(objectMap -> objectMap instanceof ObjectMap)
        .peek(objectMap -> LOG.debug("Creating term generator for ObjectMap {}", objectMap.getResourceName()))
        .map(objectMap -> {
          try {
            return termGeneratorFactory.getObjectGenerator((ObjectMap) objectMap);
          } catch (RuntimeException ex) {
            throw new TriplesMapperException(String.format("Exception occurred while creating object generator for %s",
                exception(triplesMap, objectMap)), ex);
          }
        });
  }

  private static RefObjectMap checkLogicalSource(RefObjectMap refObjectMap, LogicalSource logicalSource,
      TriplesMap triplesMap) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking if logicalSource for parent triples map {} is equal", refObjectMap.getParentTriplesMap()
          .getResourceName());
    }

    LogicalSource parentLogicalSource = refObjectMap.getParentTriplesMap()
        .getLogicalSource();

    if (parentLogicalSource == null) {
      throw new TriplesMapperException(String.format(
          "Could not determine logical source of parent TriplesMap on RefObjectMap %s%nPossibly the parent triples "
              + "map does not exist, or the reference to it is misspelled?",
          exception(triplesMap, refObjectMap)));
    }

    if (!logicalSource.equals(parentLogicalSource)) {
      throw new TriplesMapperException(String.format(
          "Logical sources are not equal.%n%nParent logical source: %s%n%nChild logical source: %s%n%nNot equal in "
              + "RefObjectMap %s",
          log(refObjectMap.getParentTriplesMap(), parentLogicalSource), log(triplesMap, logicalSource),
          exception(triplesMap, refObjectMap)));
    }

    return refObjectMap;
  }

  private static Stream<TermGenerator<? extends Value>> createJoinlessRefObjectMapGenerators(
      Set<BaseObjectMap> objectMaps, TriplesMap triplesMap, RdfTermGeneratorFactory termGeneratorFactory) {

    LogicalSource logicalSource = triplesMap.getLogicalSource();

    return objectMaps.stream()
        .filter(objectMap -> objectMap instanceof RefObjectMap)
        .peek(objectMap -> LOG.debug("Creating mapper for RefObjectMap {}", objectMap.getResourceName()))
        .map(objectMap -> (RefObjectMap) objectMap)
        .filter(refObjMap -> refObjMap.getJoinConditions()
            .isEmpty())
        // ref object maps without joins MUST have an identical logical source.
        .map(refObjMap -> checkLogicalSource(refObjMap, logicalSource, triplesMap))
        .map(refObjMap -> createRefObjectJoinlessMapper(refObjMap, triplesMap, termGeneratorFactory));
  }

  private static TermGenerator<Resource> createRefObjectJoinlessMapper(RefObjectMap refObjectMap, TriplesMap triplesMap,
      RdfTermGeneratorFactory termGeneratorFactory) {
    try {
      return termGeneratorFactory.getSubjectGenerator(refObjectMap.getParentTriplesMap()
          .getSubjectMap());
    } catch (RuntimeException ex) {
      throw new TriplesMapperException(String.format("Exception occurred for %s", exception(triplesMap, refObjectMap)),
          ex);
    }
  }

  public Flux<Statement> map(ExpressionEvaluation expressionEvaluation, Set<Resource> subjects,
      Set<Resource> subjectGraphs) {
    Set<IRI> predicates = predicateGenerators.stream()
        .map(g -> g.apply(expressionEvaluation))
        .flatMap(List::stream)
        .collect(ImmutableSet.toImmutableSet());

    if (predicates.isEmpty()) {
      return Flux.empty();
    }

    Set<Value> objects = objectGenerators.stream()
        .map(g -> g.apply(expressionEvaluation))
        .flatMap(List::stream)
        .collect(ImmutableSet.toImmutableSet());

    Set<Resource> graphs = Stream.concat(subjectGraphs.stream(), graphGenerators.stream()
        .flatMap(graphGenerator -> graphGenerator.apply(expressionEvaluation)
            .stream()))
        .collect(ImmutableSet.toImmutableSet());

    Flux<Statement> cartesianProductStatements = Flux.empty();

    if (!objects.isEmpty()) {
      cartesianProductStatements = Flux.fromStream(ModelUtil.streamCartesianProductStatements(subjects, predicates,
          objects, graphs, RdfTriplesMapper.defaultGraphModifier, valueFactory, RdfTriplesMapper.logAddStatements));
    }

    Flux<Statement> refObjectMapperPromises = Flux.merge(rdfRefObjectMappers.stream()
        .map(rdfRefObjectMapper -> rdfRefObjectMapper.map(subjects, predicates, graphs, expressionEvaluation))
        .collect(ImmutableList.toImmutableList()));

    return Flux.merge(cartesianProductStatements, refObjectMapperPromises);
  }

}
