package com.taxonic.carml.engine.rdf;

import static com.taxonic.carml.util.LogUtil.exception;
import static com.taxonic.carml.util.LogUtil.log;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.TriplesMapperException;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.ModelUtil;
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
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfSubjectMapper {

  @NonNull
  private final TermGenerator<Resource> subjectGenerator;

  private final Set<TermGenerator<Resource>> graphGenerators;

  private final Set<IRI> classes;

  @NonNull
  private final ValueFactory valueFactory;

  public static RdfSubjectMapper of(@NonNull TriplesMap triplesMap, @NonNull RdfMappingContext rdfMappingContext) {
    SubjectMap subjectMap = triplesMap.getSubjectMap();

    if (subjectMap == null) {
      throw new TriplesMapperException(
          String.format("Subject map must be specified in triples map %s", exception(triplesMap, triplesMap)));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating mapper for SubjectMap {}", log(triplesMap, subjectMap));
    }

    RdfTermGeneratorFactory rdfTermGeneratorFactory =
        (RdfTermGeneratorFactory) rdfMappingContext.getTermGeneratorFactory();

    TermGenerator<Resource> subjectGenerator;
    try {
      subjectGenerator = rdfTermGeneratorFactory.getSubjectGenerator(subjectMap);
    } catch (RuntimeException ex) {
      throw new TriplesMapperException(String.format("Exception occurred while creating subject generator for %s",
          exception(triplesMap, subjectMap)), ex);
    }

    return new RdfSubjectMapper(subjectGenerator,
        RdfTriplesMapper.createGraphGenerators(subjectMap.getGraphMaps(), rdfTermGeneratorFactory),
        subjectMap.getClasses(), rdfMappingContext.getValueFactorySupplier()
            .get());
  }

  public Result map(ExpressionEvaluation expressionEvaluation) {
    LOG.debug("Determining subjects ...");
    Set<Resource> subjects = subjectGenerator.apply(expressionEvaluation)
        .stream()
        .collect(ImmutableSet.toImmutableSet());

    LOG.debug("Determined subjects {}", subjects);

    if (subjects.isEmpty()) {
      return resultOf(subjects, ImmutableSet.of(), Flux.empty());
    }

    // graphs to be used when generating statements in predicate object mapper
    Set<Resource> graphs = graphGenerators.stream()
        .flatMap(graph -> graph.apply(expressionEvaluation)
            .stream())
        .collect(ImmutableSet.toImmutableSet());

    Flux<Statement> typeStatements = classes.isEmpty() ? Flux.empty() : mapTypeStatements(subjects, graphs);

    return resultOf(subjects, graphs, typeStatements);
  }

  private Flux<Statement> mapTypeStatements(Set<Resource> subjects, Set<Resource> graphs) {
    LOG.debug("Generating triples for subjects: {}", subjects);

    Stream<Statement> typeStatementStream = ModelUtil.streamCartesianProductStatements(subjects, Set.of(RDF.TYPE),
        classes, graphs, RdfTriplesMapper.defaultGraphModifier, valueFactory, RdfTriplesMapper.logAddStatements);

    return Flux.fromStream(typeStatementStream);
  }

  private static Result resultOf(Set<Resource> subjects, Set<Resource> graphs, Flux<Statement> typeStatements) {
    return new Result(subjects, graphs, typeStatements);
  }

  @Getter
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  static class Result {
    Set<Resource> subjects;

    Set<Resource> graphs;

    Flux<Statement> typeStatements;
  }

}
