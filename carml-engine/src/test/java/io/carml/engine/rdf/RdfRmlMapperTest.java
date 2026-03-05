package io.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.carml.engine.CompositeObserver;
import io.carml.engine.MappingExecution;
import io.carml.engine.MappingExecutionObserver;
import io.carml.engine.NoOpObserver;
import io.carml.engine.RmlMapperException;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolverException;
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
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
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
                .allowMultipleSubjectMaps(true)
                .valueFactorySupplier(ValidatingValueFactory::new)
                .classPathResolver("classpath")
                .fileResolver(Path.of("file"))
                .iriUpperCasePercentEncoding(true)
                .iriUnicodeNormalization(Normalizer.Form.NFKC)
                .addFunctions(new Object())
                .baseIri("https://example.com/");

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

        // When — map() returns a lazy Flux; the error surfaces during subscription
        var rmlMapperException = assertThrows(RmlMapperException.class, rmlMapper::mapToModel);

        // Then
        assertThat(
                rmlMapperException.getMessage(),
                is("Could not resolve input stream with name DEFAULT for logical view source"));
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
                is("Could not resolve input stream with name cars for logical view source"));
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

        // When — map() returns a lazy Flux; the error surfaces during subscription
        var rmlMapperException = assertThrows(SourceResolverException.class, rmlMapper::mapToModel);

        // Then
        assertThat(rmlMapperException.getMessage(), startsWith("File does not exist at path bar/cars.csv for source"));
    }

    // -- addFunctionDescriptions --

    @Test
    void addFunctionDescriptions_buildsSuccessfully_givenValidFnoModel() {
        var mapping = loadMapping("mapping.rml.ttl");
        var fnoModel = createMinimalFnoModel();
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .allowMultipleSubjectMaps(true)
                .addFunctionDescriptions(fnoModel)
                .build();

        assertThat(rmlMapper, is(notNullValue()));
    }

    @Test
    void addFunctionDescriptions_buildsSuccessfully_givenValidInputStream() {
        var mapping = loadMapping("mapping.rml.ttl");
        var turtle = String.join(
                "\n",
                "@prefix fno: <https://w3id.org/function/ontology#> .",
                "@prefix fnoi: <https://w3id.org/function/vocabulary/implementation#> .",
                "@prefix fnom: <https://w3id.org/function/vocabulary/mapping#> .",
                "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
                "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
                "@prefix ex: <http://example.com/> .",
                "",
                "ex:toLowercase a fno:Function ;",
                "  fno:expects ( ex:param1 ) .",
                "",
                "ex:param1 fno:predicate ex:startString ;",
                "  fno:type xsd:string .",
                "",
                "ex:fnoMapping a fno:Mapping ;",
                "  fno:function ex:toLowercase ;",
                "  fno:implementation ex:javaClass ;",
                "  fno:methodMapping [ fnom:method-name \"toLowercase\" ] .",
                "",
                "ex:javaClass fnoi:class-name \"io.carml.engine.iotests.RmlFunctions\" .");

        var inputStream = new java.io.ByteArrayInputStream(turtle.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .allowMultipleSubjectMaps(true)
                .addFunctionDescriptions(inputStream, RDFFormat.TURTLE)
                .build();

        assertThat(rmlMapper, is(notNullValue()));
    }

    @Test
    void addFunctionDescriptions_throwsRmlMapperException_givenInvalidRdf() {
        var invalidRdf = new java.io.ByteArrayInputStream("this is not valid RDF".getBytes());
        var builder = RdfRmlMapper.builder();

        assertThrows(RmlMapperException.class, () -> builder.addFunctionDescriptions(invalidRdf, RDFFormat.TURTLE));
    }

    // -- addFunctionClasses --

    @Test
    void addFunctionClasses_buildsSuccessfully_givenValidClassName() {
        var mapping = loadMapping("mapping.rml.ttl");
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .allowMultipleSubjectMaps(true)
                .addFunctionClasses("io.carml.engine.iotests.RmlFunctions")
                .build();

        assertThat(rmlMapper, is(notNullValue()));
    }

    @Test
    void addFunctionClasses_throwsRmlMapperException_givenNonExistentClass() {
        var builder = RdfRmlMapper.builder();

        assertThrows(RmlMapperException.class, () -> builder.addFunctionClasses("com.example.DoesNotExist"));
    }

    @Test
    void addFunctionClasses_buildsSuccessfully_givenEmptyArray() {
        var mapping = loadMapping("mapping.rml.ttl");
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .allowMultipleSubjectMaps(true)
                .addFunctionClasses()
                .build();

        assertThat(rmlMapper, is(notNullValue()));
    }

    // -- function() fluent chain --

    @Test
    void functionBuilder_buildsSuccessfully_givenLambdaFunction() {
        var mapping = loadMapping("mapping.rml.ttl");
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .allowMultipleSubjectMaps(true)
                .function("http://example.org/fn")
                .param("http://example.org/p", String.class)
                .returns(String.class)
                .execute(params -> "ok")
                .build();

        assertThat(rmlMapper, is(notNullValue()));
    }

    @Test
    void functionBuilder_buildsSuccessfully_withOptionalParam() {
        var mapping = loadMapping("mapping.rml.ttl");
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .allowMultipleSubjectMaps(true)
                .function("http://example.org/fn")
                .param("http://example.org/required", String.class)
                .optionalParam("http://example.org/optional", String.class)
                .returns(String.class)
                .execute(params -> "ok")
                .build();

        assertThat(rmlMapper, is(notNullValue()));
    }

    private Set<TriplesMap> loadMapping(String resourceName) {
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream(resourceName);
        return RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
    }

    private static TriplesMap getTriplesMapByName(String name, Set<TriplesMap> mapping) {
        return mapping.stream()
                .filter(tm -> tm.getResourceName().equals(name))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
    }

    /**
     * Creates a minimal FnO model with a simple toUpperCase function backed by
     * {@link io.carml.engine.iotests.RmlFunctions}.
     */
    private static Model createMinimalFnoModel() {
        var vf = SimpleValueFactory.getInstance();
        var model = new TreeModel();

        var fnoNs = "https://w3id.org/function/ontology#";
        var fnoiNs = "https://w3id.org/function/vocabulary/implementation#";
        var fnomNs = "https://w3id.org/function/vocabulary/mapping#";
        var ex = "http://example.com/";

        IRI fnoFunction = vf.createIRI(fnoNs + "Function");
        IRI fnoExpects = vf.createIRI(fnoNs + "expects");
        IRI fnoPredicate = vf.createIRI(fnoNs + "predicate");
        IRI fnoType = vf.createIRI(fnoNs + "type");
        IRI fnoMapping = vf.createIRI(fnoNs + "Mapping");
        IRI fnoFunctionProp = vf.createIRI(fnoNs + "function");
        IRI fnoImplementation = vf.createIRI(fnoNs + "implementation");
        IRI fnoMethodMapping = vf.createIRI(fnoNs + "methodMapping");
        IRI fnoiClassName = vf.createIRI(fnoiNs + "class-name");
        IRI fnomMethodName = vf.createIRI(fnomNs + "method-name");

        IRI toLowerIri = vf.createIRI(ex + "toLowercase");
        IRI startStringIri = vf.createIRI(ex + "startString");

        // Parameter
        var param = vf.createIRI(ex + "param1");
        model.add(param, fnoPredicate, startStringIri);
        model.add(param, fnoType, XSD.STRING);

        // Expects list (single element)
        var listNode = vf.createIRI(ex + "list1");
        model.add(listNode, RDF.FIRST, param);
        model.add(listNode, RDF.REST, RDF.NIL);

        // Function
        model.add(toLowerIri, RDF.TYPE, fnoFunction);
        model.add(toLowerIri, fnoExpects, listNode);

        // Implementation
        var javaClass = vf.createIRI(ex + "javaClass");
        model.add(javaClass, fnoiClassName, vf.createLiteral("io.carml.engine.iotests.RmlFunctions"));

        // Method mapping
        var mm = vf.createBNode();
        model.add(mm, fnomMethodName, vf.createLiteral("toLowercase"));

        // Mapping
        var mapping = vf.createIRI(ex + "fnoMapping");
        model.add(mapping, RDF.TYPE, fnoMapping);
        model.add(mapping, fnoFunctionProp, toLowerIri);
        model.add(mapping, fnoImplementation, javaClass);
        model.add(mapping, fnoMethodMapping, mm);

        return model;
    }

    // --- Builder.limit() validation ---

    @Test
    void givenBuilderWithZeroLimit_whenLimit_thenThrowIllegalArgumentException() {
        var builder = RdfRmlMapper.builder();
        var exception = assertThrows(IllegalArgumentException.class, () -> builder.limit(0));
        assertThat(exception.getMessage(), startsWith("limit must be positive"));
    }

    @Test
    void givenBuilderWithNegativeLimit_whenLimit_thenThrowIllegalArgumentException() {
        var builder = RdfRmlMapper.builder();
        var exception = assertThrows(IllegalArgumentException.class, () -> builder.limit(-5));
        assertThat(exception.getMessage(), startsWith("limit must be positive"));
    }

    @Test
    void givenBuilderWithPositiveLimit_whenLimit_thenReturnsBuilder() {
        var builder = RdfRmlMapper.builder();
        var result = builder.limit(10);
        assertThat(result, is(notNullValue()));
    }

    // --- start() ---

    @Test
    void givenMappingExpectingFileSource_whenStart_thenReturnsMappingExecution() {
        // Given
        var rmlMapper = RdfRmlMapper.builder()
                .mapping(Mapping.of(RDFFormat.TURTLE, this.getClass(), "cars-file-input.rml.ttl"))
                .classPathResolver(ClassPathResolver.of(RdfRmlMapperTest.class))
                .build();

        // When
        var execution = rmlMapper.start();

        // Then
        assertThat(execution, is(notNullValue()));
        assertThat(execution, is(instanceOf(MappingExecution.class)));
    }

    @Test
    void givenMappingExpectingFileSource_whenStartAndConsumeStatements_thenProducesStatements() {
        // Given
        var rmlMapper = RdfRmlMapper.builder()
                .mapping(Mapping.of(RDFFormat.TURTLE, this.getClass(), "cars-file-input.rml.ttl"))
                .classPathResolver(ClassPathResolver.of(RdfRmlMapperTest.class))
                .build();

        // When
        var execution = rmlMapper.start();

        // Then
        StepVerifier.create(execution.statements())
                .expectNextCount(22)
                .expectComplete()
                .verify();
    }

    @Test
    void givenMappingExpectingFileSource_whenStartAndConsumeStatements_thenMetricsReflectCount() {
        // Given
        var rmlMapper = RdfRmlMapper.builder()
                .mapping(Mapping.of(RDFFormat.TURTLE, this.getClass(), "cars-file-input.rml.ttl"))
                .classPathResolver(ClassPathResolver.of(RdfRmlMapperTest.class))
                .build();

        var execution = rmlMapper.start();
        execution.statements().blockLast();

        // When
        var metrics = execution.currentMetrics();

        // Then
        assertThat(metrics.statementsProduced(), is(22L));
        assertThat(metrics.errorsEncountered(), is(0L));
    }

    @Test
    void givenMappingExpectingFileSource_whenStartAndCancelBeforeSubscribe_thenFluxCompletesImmediately() {
        // Given
        var rmlMapper = RdfRmlMapper.builder()
                .mapping(Mapping.of(RDFFormat.TURTLE, this.getClass(), "cars-file-input.rml.ttl"))
                .classPathResolver(ClassPathResolver.of(RdfRmlMapperTest.class))
                .build();

        var execution = rmlMapper.start();

        // When — cancel before subscribing to statements
        execution.cancel().block();

        // Then — flux completes immediately with no elements
        StepVerifier.create(execution.statements()).expectComplete().verify();
    }

    // --- Observer wiring ---

    @Test
    void givenNoObserver_whenBuild_thenGetObserverReturnsNoOp() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("mapping.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);

        // When
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .allowMultipleSubjectMaps(true)
                .build();

        // Then
        assertThat(rmlMapper.getObserver(), is(instanceOf(NoOpObserver.class)));
    }

    @Test
    void givenSingleObserver_whenBuild_thenGetObserverReturnsThatObserver() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("mapping.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var observer = mock(MappingExecutionObserver.class);

        // When
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .allowMultipleSubjectMaps(true)
                .observer(observer)
                .build();

        // Then
        assertThat(rmlMapper.getObserver(), is(sameInstance(observer)));
    }

    @Test
    void givenMultipleObservers_whenBuild_thenGetObserverReturnsCompositeObserver() {
        // Given
        var mappingSource = RdfRmlMapperTest.class.getResourceAsStream("mapping.rml.ttl");
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingSource);
        var observer1 = mock(MappingExecutionObserver.class);
        var observer2 = mock(MappingExecutionObserver.class);

        // When
        var rmlMapper = RdfRmlMapper.builder()
                .triplesMaps(mapping)
                .allowMultipleSubjectMaps(true)
                .observer(observer1)
                .observer(observer2)
                .build();

        // Then
        assertThat(rmlMapper.getObserver(), is(instanceOf(CompositeObserver.class)));
    }
}
