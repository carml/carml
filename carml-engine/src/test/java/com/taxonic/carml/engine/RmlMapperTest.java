package com.taxonic.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;

import com.taxonic.carml.engine.rdf.ModelResult;
import com.taxonic.carml.engine.rdf.RdfRmlMapperBuilder;
import com.taxonic.carml.logical_source_resolver.CsvResolver;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.ModelSerializer;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.ORG;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;
import reactor.core.scheduler.Schedulers;

class RmlMapperTest {

  @Test
  public void blockHoundWorks() {
    try {
      FutureTask<?> task = new FutureTask<>(() -> {
        Thread.sleep(0);
        return "";
      });
      Schedulers.parallel()
          .schedule(task);

      task.get(10, TimeUnit.SECONDS);
      assertThat("should fail", false);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      assertThat("detected", e.getCause() instanceof BlockingOperationError);
    }
  }

  @Test
  void mapperTest() {

    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, RmlMapperTest.class.getResourceAsStream("cars.rml.ttl"));

    RmlMapper<Statement> mapper =
        new RdfRmlMapperBuilder().setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
            .triplesMaps(mapping)
            .build();

    Model model = ModelResult.from(mapper.map(RmlMapperTest.class.getResourceAsStream("cars.csv")));
    System.out.println(ModelSerializer.serializeAsRdf(model, RDFFormat.TURTLE));
  }

  @Test
  void mapperTestMappingWithJoin() {

    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, RmlMapperTest.class.getResourceAsStream("cars.join.rml.ttl"));

    RmlMapper<Statement> mapper =
        new RdfRmlMapperBuilder().setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
            .triplesMaps(mapping)
            .build();

    Model model = ModelResult.from(mapper.map(RmlMapperTest.class.getResourceAsStream("cars.csv")));
    System.out.println(ModelSerializer.serializeAsRdf(model, RDFFormat.TURTLE));
  }

  @Test
  void csvMapperTest() {

    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, RmlMapperTest.class.getResourceAsStream("openbare-lichamen.rml.ttl"));

    RmlMapper<Statement> mapper =
        new RdfRmlMapperBuilder().setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
            .triplesMaps(mapping)
            .build();

    Model model = ModelResult.from(mapper.map(RmlMapperTest.class.getResourceAsStream("openbare-lichamen.csv")));

    model.setNamespace("bi", "http://data.pdok.nl/def/bi#");
    model.setNamespace(ORG.NS);
    System.out.println(ModelSerializer.serializeAsRdf(model, RDFFormat.TURTLE));
  }
}
