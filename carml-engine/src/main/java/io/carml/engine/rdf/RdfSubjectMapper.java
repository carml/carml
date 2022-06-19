package io.carml.engine.rdf;

import static io.carml.util.LogUtil.exception;
import static io.carml.util.LogUtil.log;

import io.carml.engine.ExpressionEvaluation;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.util.Models;
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

  public static RdfSubjectMapper of(@NonNull SubjectMap subjectMap, @NonNull TriplesMap triplesMap,
      @NonNull RdfMappingContext rdfMappingContext) {
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
        .collect(Collectors.toUnmodifiableSet());

    LOG.debug("Determined subjects {}", subjects);

    if (subjects.isEmpty()) {
      return resultOf(subjects, Set.of(), Flux.empty());
    }

    // graphs to be used when generating statements in predicate object mapper
    Set<Resource> graphs = graphGenerators.stream()
        .flatMap(graph -> graph.apply(expressionEvaluation)
            .stream())
        .collect(Collectors.toUnmodifiableSet());

    Flux<Statement> typeStatements = classes.isEmpty() ? Flux.empty() : mapTypeStatements(subjects, graphs);

    return resultOf(subjects, graphs, typeStatements);
  }

  private Flux<Statement> mapTypeStatements(Set<Resource> subjects, Set<Resource> graphs) {
    LOG.debug("Generating triples for subjects: {}", subjects);

    Stream<Statement> typeStatementStream = Models.streamCartesianProductStatements(subjects, Set.of(RDF.TYPE), classes,
        graphs, RdfTriplesMapper.defaultGraphModifier, valueFactory, RdfTriplesMapper.logAddStatements);

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
