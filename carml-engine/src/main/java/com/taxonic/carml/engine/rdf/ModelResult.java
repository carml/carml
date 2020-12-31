package com.taxonic.carml.engine.rdf;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ModelResult {

  private static final Duration TIME_OUT = Duration.of(600, ChronoUnit.SECONDS);

  public static Model from(Flux<Statement> statements) {
    return from(statements, new LinkedHashModel());
  }

  public static Model from(Flux<Statement> statements, Supplier<Model> modelSupplier) {
    return from(statements, modelSupplier.get());
  }

  public static Model from(Flux<Statement> statements, Model toPopulate) {
    Disposable result = statements.subscribe(toPopulate::add);
    statements.blockLast(TIME_OUT);

    result.dispose();

    return toPopulate;
  }

}
