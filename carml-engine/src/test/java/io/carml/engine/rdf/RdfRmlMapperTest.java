package io.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.engine.RmlMapperException;
import io.carml.engine.join.impl.CarmlChildSideJoinStoreProvider;
import io.carml.engine.join.impl.CarmlParentSideJoinConditionStoreProvider;
import io.carml.logicalsourceresolver.CsvResolver;
import io.carml.logicalsourceresolver.XPathResolver;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolver;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseConnectionOptions;
import io.carml.model.TriplesMap;
import io.carml.util.RmlMappingLoader;
import io.carml.vocab.Rdf;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfRmlMapperTest {

  @Test
  void givenBuilderWithNoLogicalSourceResolver_whenBuild_thenThrowException() {
    // Given
    RdfRmlMapper.Builder builder = RdfRmlMapper.builder();

    // When
    RmlMapperException rmlMapperException = assertThrows(RmlMapperException.class, builder::build);

    // Then
    assertThat(rmlMapperException.getMessage(), is("No logical source resolver suppliers specified."));
  }

  @Test
  void givenBuilderWithNoMapping_whenBuild_thenThrowException() {
    // Given
    RdfRmlMapper.Builder builder = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance);

    // When
    RmlMapperException rmlMapperException = assertThrows(RmlMapperException.class, builder::build);

    // Then
    assertThat(rmlMapperException.getMessage(), is("No executable triples maps found."));
  }

  @Test
  void givenBuilderWithMappingWithUnsupportedReferenceFormulation_whenBuild_thenThrowException() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("mapping.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper.Builder builder = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.JsonPath, XPathResolver::getInstance)
        .triplesMaps(mapping);

    // When
    RmlMapperException rmlMapperException = assertThrows(RmlMapperException.class, builder::build);

    // Then
    assertThat(rmlMapperException.getMessage(),
        equalToCompressingWhiteSpace("No logical source resolver supplier bound for reference formulation "
            + "http://semweb.mmlab.be/ns/ql#XPath Resolvers available: http://semweb.mmlab.be/ns/ql#JSONPath"));
  }

  @Test
  void givenAllOptions_whenBuild_thenBuildMapperCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("mapping.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper.Builder builder = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance)
        .triplesMaps(mapping)
        .valueFactorySupplier(ValidatingValueFactory::new)
        .classPathResolver("classpath")
        .fileResolver(Path.of("file"))
        .iriUpperCasePercentEncoding(true)
        .iriUnicodeNormalization(Normalizer.Form.NFKC)
        .childSideJoinStoreProvider(CarmlChildSideJoinStoreProvider.of())
        .parentSideJoinConditionStoreProvider(CarmlParentSideJoinConditionStoreProvider.of())
        .addFunctions(new Object())
        .sourceResolver(new SourceResolver() {
          @Override
          public boolean supportsSource(Object sourceObject) {
            return true;
          }

          @Override
          public Optional<Object> apply(Object o) {
            return Optional.empty();
          }
        })
        .baseIri("https://example.com/")
        .databaseConnectionOptions(DatabaseConnectionOptions.builder()
            .database("db://")
            .username("foo")
            .password("bar")
            .build())
        .logicalSourceResolverMatcher(CsvResolver.Matcher.getInstance());

    // When
    RdfRmlMapper rmlMapper = builder.build();

    // Then
    assertThat(rmlMapper.getTriplesMaps(), is(mapping));
  }

  @Test
  void givenMappingExpectingInputStream_whenMapCalledWithoutInputStream_thenThrowException() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .build();

    // When
    RmlMapperException rmlMapperException = assertThrows(RmlMapperException.class, rmlMapper::map);

    // Then
    assertThat(rmlMapperException.getMessage(), is("Could not resolve input stream with name DEFAULT for logical"
        + " source: resource <http://example.com/mapping/LogicalSource>"));
  }

  @Test
  void givenMappingExpectingInputStream_whenMapWithInputStream_thenMapCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .build();

    InputStream sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

    // When
    Flux<Statement> statements = rmlMapper.map(sourceInputStream);

    // Then
    StepVerifier.create(statements)
        .expectNextCount(22)
        .expectComplete()
        .verify();
  }

  @Test
  void givenMappingExpectingInputStream_whenMapToModelWithInputStream_thenMapCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .build();

    InputStream sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

    // When
    Model model = rmlMapper.mapToModel(sourceInputStream);

    // Then
    assertThat(model.size(), is(21));
  }

  @Test
  void givenMappingExpectingInputStreamAndTriplesMapFilter_whenMapToModelWithInputStreamAndFilter_thenMapCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);

    TriplesMap makeMapping = mapping.stream()
        .filter(tm -> tm.getResourceName()
            .equals("http://example.com/mapping/MakeMapping"))
        .findFirst()
        .orElseThrow(IllegalStateException::new);

    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .build();

    InputStream sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

    // When
    Model model = rmlMapper.mapToModel(sourceInputStream, Set.of(makeMapping));

    // Then
    assertThat(model.size(), is(3));
  }

  @Test
  void givenMappingExpectingInputStream_whenMapRecordToModelWithInputStream_thenMapCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .build();

    InputStream sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

    // When
    Model model = rmlMapper.mapRecordToModel(sourceInputStream, InputStream.class);

    // Then
    assertThat(model.size(), is(21));
  }

  @Test
  void givenMappingExpectingInputStreamAndFilter_whenMapRecordToModelWithInputStreamAndFilter_thenMapCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);

    TriplesMap makeMapping = getTriplesMapByName("http://example.com/mapping/MakeMapping", mapping);

    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .build();

    InputStream sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

    // When
    Model model = rmlMapper.mapRecordToModel(sourceInputStream, InputStream.class, Set.of(makeMapping));

    // Then
    assertThat(model.size(), is(3));
  }

  @Test
  void givenMappingExpectingNamedInputStream_whenMapToModelWithInputStream_thenMapCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-stream-name.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .build();

    Map<String, InputStream> namedInputStream = Map.of("cars", RdfRmlMapperTest.class.getResourceAsStream("cars.csv"));

    // When
    Model model = rmlMapper.mapToModel(namedInputStream);

    // Then
    assertThat(model.size(), is(21));
  }

  @Test
  void givenMappingExpectingNamedInputStream_whenMapToModelWithoutThatInputStream_thenThrowException() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-stream-name.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .build();

    Map<String, InputStream> namedInputStream = Map.of("foo", RdfRmlMapperTest.class.getResourceAsStream("cars.csv"));

    // When
    RmlMapperException rmlMapperException =
        assertThrows(RmlMapperException.class, () -> rmlMapper.mapToModel(namedInputStream));

    // Then
    assertThat(rmlMapperException.getMessage(), is("Could not resolve input stream with name cars for logical"
        + " source: resource <http://example.com/mapping/LogicalSource>"));
  }

  @Test
  void givenMappingExpectingNamedInputStreamAndFilter_whenMapRecordToModelWithInputStreamAndFilter_thenMapCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-stream-name.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);

    TriplesMap makeMapping = getTriplesMapByName("http://example.com/mapping/MakeMapping", mapping);

    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .build();

    Map<String, InputStream> namedInputStream = Map.of("cars", RdfRmlMapperTest.class.getResourceAsStream("cars.csv"));

    // When
    Model model = rmlMapper.mapToModel(namedInputStream, Set.of(makeMapping));

    // Then
    assertThat(model.size(), is(3));
  }

  @Test
  void givenMappingExpectingFileSource_whenMapToModel_thenMapCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-file-input.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .classPathResolver(ClassPathResolver.of(RdfRmlMapperTest.class))
        .build();

    // When
    Model model = rmlMapper.mapToModel();

    // Then
    assertThat(model.size(), is(21));
  }

  @Test
  void givenMappingExpectingFileSourceAndFilter_whenMapRecordToModelWithFilter_thenMapCorrectly() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-file-input.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);

    TriplesMap makeMapping = getTriplesMapByName("http://example.com/mapping/MakeMapping", mapping);

    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .classPathResolver(ClassPathResolver.of(RdfRmlMapperTest.class))
        .build();

    // When
    Model model = rmlMapper.mapToModel(Set.of(makeMapping));

    // Then
    assertThat(model.size(), is(3));
  }

  @Test
  void givenMappingWithUnresolvableSource_whenMapCalled_thenThrowException() {
    // Given
    InputStream mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-file-input.rml.ttl");
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingSource);
    RdfRmlMapper rmlMapper = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .triplesMaps(mapping)
        .classPathResolver("foo")
        .fileResolver(Paths.get("bar"))
        .build();

    // When
    RmlMapperException rmlMapperException = assertThrows(RmlMapperException.class, rmlMapper::map);

    // Then
    assertThat(rmlMapperException.getMessage(),
        is("Could not resolve source for logical source: resource <http://example.com/mapping/LogicalSource>"));
  }

  private static TriplesMap getTriplesMapByName(String name, Set<TriplesMap> mapping) {
    return mapping.stream()
        .filter(tm -> tm.getResourceName()
            .equals(name))
        .findFirst()
        .orElseThrow(IllegalStateException::new);
  }

}
