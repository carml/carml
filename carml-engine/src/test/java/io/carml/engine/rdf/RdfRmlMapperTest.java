package io.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.engine.RmlMapperException;
import io.carml.engine.join.impl.CarmlChildSideJoinStoreProvider;
import io.carml.engine.join.impl.CarmlParentSideJoinConditionStoreProvider;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolverException;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseConnectionOptions;
import io.carml.model.Mapping;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.impl.CarmlMapperException;
import io.carml.util.RmlMappingLoader;
import io.carml.util.TypeRef;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfRmlMapperTest {

    @Test
    void givenBuilderWithNoMapping_whenBuild_thenThrowException() {
        // Given
        var builder = RdfRmlMapper.builder();

        // When
        var rmlMapperException = assertThrows(RmlMapperException.class, builder::build);

        // Then
        assertThat(rmlMapperException.getMessage(), is("No executable triples maps found."));
    }

    @Test
    void givenBuilderWithMappingWithUnsupportedReferenceFormulation_whenBuild_thenThrowException() {
        // Given // When
        var exception = assertThrows(
                CarmlMapperException.class,
                () -> Mapping.of(RDFFormat.TURTLE, RdfRmlMapperTest.class, "unsupported-ref-formulation.rml.ttl"));

        // Then
        assertThat(
                exception.getMessage(),
                is("Encountered unsupported reference formulation: http://semweb.mmlab.be/ns/ql#Foo"));
    }

    @Test
    void givenAllOptions_whenBuild_thenBuildMapperCorrectly() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("mapping.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var builder = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .valueFactorySupplier(ValidatingValueFactory::new)
                .classPathResolver("classpath")
                .fileResolver(Path.of("file"))
                .iriUpperCasePercentEncoding(true)
                .iriUnicodeNormalization(Normalizer.Form.NFKC)
                .childSideJoinStoreProvider(CarmlChildSideJoinStoreProvider.of())
                .parentSideJoinConditionStoreProvider(CarmlParentSideJoinConditionStoreProvider.of())
                .addFunctions(new Object())
                .baseIri("https://example.com/")
                .databaseConnectionOptions(DatabaseConnectionOptions.builder()
                        .database("db://")
                        .username("foo")
                        .password("bar")
                        .build());

        // When
        var rmlMapper = builder.build();

        // Then
        assertThat(rmlMapper.getTriplesMaps(), is(mapping));
    }

    @Test
    void givenMappingExpectingInputStream_whenMapCalledWithoutInputStream_thenThrowException() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        // When
        var rmlMapperException = assertThrows(RmlMapperException.class, rmlMapper::map);

        // Then
        assertThat(
                rmlMapperException.getMessage(),
                is("Could not resolve input stream with name DEFAULT for logical"
                        + " source: resource <http://example.com/mapping/LogicalSource>"));
    }

    @Test
    void givenMappingExpectingInputStream_whenMapWithInputStream_thenMapCorrectly() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        var sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

        // When
        var statements = rmlMapper.map(sourceInputStream);

        // Then
        StepVerifier.create(statements).expectNextCount(22).expectComplete().verify();
    }

    @Test
    void givenMappingExpectingInputStream_whenMapToModelWithInputStream_thenMapCorrectly() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        var sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

        // When
        var model = rmlMapper.mapToModel(sourceInputStream);

        // Then
        assertThat(model.size(), is(21));
    }

    @Test
    void givenMappingExpectingInputStreamAndTriplesMapFilter_whenMapToModelWithInputStreamAndFilter_thenMapCorrectly() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);

        var makeMapping = mapping.stream()
                .filter(tm -> tm.getResourceName().equals("http://example.com/mapping/MakeMapping"))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        var sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

        // When
        var model = rmlMapper.mapToModel(sourceInputStream, Set.of(makeMapping));

        // Then
        assertThat(model.size(), is(3));
    }

    @Test
    void givenMappingExpectingInputStream_whenMapRecordToModelWithInputStream_thenMapCorrectly() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        var sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

        // When
        var model = rmlMapper.mapRecordToModel(Mono.just(sourceInputStream), new TypeRef<>() {});

        // Then
        assertThat(model.size(), is(21));
    }

    @Test
    void givenMappingExpectingInputStreamAndFilter_whenMapRecordToModelWithInputStreamAndFilter_thenMapCorrectly() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);

        var makeMapping = getTriplesMapByName("http://example.com/mapping/MakeMapping", mapping);

        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        var sourceInputStream = RdfRmlMapperTest.class.getResourceAsStream("cars.csv");

        // When
        var model = rmlMapper.mapRecordToModel(Mono.just(sourceInputStream), new TypeRef<>() {}, Set.of(makeMapping));

        // Then
        assertThat(model.size(), is(3));
    }

    @Test
    void givenMappingExpectingNamedInputStream_whenMapToModelWithInputStream_thenMapCorrectly() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-stream-name.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        var namedInputStream = Map.of("cars", RdfRmlMapperTest.class.getResourceAsStream("cars.csv"));

        // When
        var model = rmlMapper.mapToModel(namedInputStream);

        // Then
        assertThat(model.size(), is(21));
    }

    @Test
    void givenMappingExpectingNamedInputStream_whenMapToModelWithoutThatInputStream_thenThrowException() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-stream-name.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        var namedInputStream = Map.of("foo", RdfRmlMapperTest.class.getResourceAsStream("cars.csv"));

        // When
        var rmlMapperException = assertThrows(RmlMapperException.class, () -> rmlMapper.mapToModel(namedInputStream));

        // Then
        assertThat(
                rmlMapperException.getMessage(),
                is("Could not resolve input stream with name cars for logical"
                        + " source: resource <http://example.com/mapping/LogicalSource>"));
    }

    @Test
    void
            givenMappingExpectingNamedInputStreamAndFilter_whenMapRecordToModelWithInputStreamAndFilter_thenMapCorrectly() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-stream-name.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);

        var makeMapping = getTriplesMapByName("http://example.com/mapping/MakeMapping", mapping);

        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        var namedInputStream = Map.of("cars", RdfRmlMapperTest.class.getResourceAsStream("cars.csv"));

        // When
        var model = rmlMapper.mapToModel(namedInputStream, Set.of(makeMapping));

        // Then
        assertThat(model.size(), is(3));
    }

    @Test
    void givenMappingExpectingFileSource_whenMapToModel_thenMapCorrectly() {
        // Given
        var rmlMapper = RdfRmlMapper.builder()
                .mapping(Mapping.of(RDFFormat.TURTLE, this.getClass(), "cars-file-input.rml.ttl"))
                .classPathResolver(ClassPathResolver.of(RdfRmlMapperTest.class))
                .build();

        // When
        var model = rmlMapper.mapToModel();

        // Then
        assertThat(model.size(), is(21));
    }

    @Test
    void givenMappingExpectingFileSourceAndFilter_whenMapRecordToModelWithFilter_thenMapCorrectly() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-file-input.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);

        var makeMapping = getTriplesMapByName("http://example.com/mapping/MakeMapping", mapping);

        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(RdfRmlMapperTest.class))
                .build();

        // When
        var model = rmlMapper.mapToModel(Set.of(makeMapping));

        // Then
        assertThat(model.size(), is(3));
    }

    @Test
    void givenMappingWithUnresolvableSource_whenMapCalled_thenThrowException() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("cars-file-input.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .classPathResolver("foo")
                .fileResolver(Paths.get("bar"))
                .build();

        // When
        var rmlMapperException = assertThrows(SourceResolverException.class, rmlMapper::map);

        // Then
        assertThat(rmlMapperException.getMessage(), startsWith("File does not exist at path bar/cars.csv for source"));
    }

    private static TriplesMap getTriplesMapByName(String name, Set<TriplesMap> mapping) {
        return mapping.stream()
                .filter(tm -> tm.getResourceName().equals(name))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
    }
}
