package io.carml.logicalsourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import de.siegmar.fastcsv.reader.CsvParseException;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver.LogicalSourceResolverFactory;
import io.carml.model.CsvReferenceFormulation;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.impl.CarmlFilePath;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.util.TypeRef;
import io.carml.vocab.Rdf.Rml;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class CsvResolverTest {

    private static final CsvReferenceFormulation CSV =
            CsvReferenceFormulation.builder().id(Rml.Csv.stringValue()).build();

    private static final Source RML_SOURCE = CarmlFilePath.of("foo");

    private static final String SOURCE = """
                    Year,Make,Model,Description,Price
                    1997,Ford,E350,"ac, abs, moon",3000.00
                    1999,Chevy,"Venture ""Extended Edition""\","",4900.00""";

    private static final LogicalSource LSOURCE = CarmlLogicalSource.builder()
            .source(RML_SOURCE)
            .referenceFormulation(CSV)
            .build();

    private static final LogicalSource LSOURCE_DELIM = CarmlLogicalSource.builder()
            .source(RML_SOURCE)
            .referenceFormulation(CSV)
            .build();

    private final Function<Object, Mono<InputStream>> sourceResolver =
            s -> Mono.just(IOUtils.toInputStream((String) s, StandardCharsets.UTF_8));

    private LogicalSourceResolverFactory<NamedCsvRecord> csvResolverFactory;

    @BeforeEach
    void init() {
        csvResolverFactory = CsvResolver.factory();
    }

    @Test
    void givenCsv_whenRecordResolverApplied_thenReturnFluxOfAllRecords() {
        // Given
        var csvResolver = csvResolverFactory.apply(LSOURCE.getSource());
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(SOURCE), new TypeRef<>() {});

        // When
        var recordFlux = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(recordFlux).expectNextCount(2).verifyComplete();
    }

    @Test
    void givenCsv_whenRecordResolverAppliedTwice_thenReturnFluxOfAllRecords() {
        // Given
        var csvResolver = csvResolverFactory.apply(LSOURCE.getSource());
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(SOURCE), new TypeRef<>() {});

        // When
        var recordFlux = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(recordFlux).expectNextCount(2).verifyComplete();

        // Given
        var recordResolver2 = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource2 = ResolvedSource.of(sourceResolver.apply(SOURCE), new TypeRef<>() {});

        // When
        var recordFlux2 = recordResolver2.apply(resolvedSource2);

        // Then
        StepVerifier.create(recordFlux2).expectNextCount(2).verifyComplete();
    }

    @Test
    void givenRandomDelimitedCsv_whenRecordResolverApplied_thenReturnFluxOfAllCorrectRecords() {
        // Given
        var csvResolver = csvResolverFactory.apply(LSOURCE_DELIM.getSource());
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE_DELIM));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(SOURCE), new TypeRef<>() {});

        // When
        var recordFlux = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(recordFlux)
                .expectNextMatches(logicalSourceRecord ->
                        logicalSourceRecord.getSourceRecord().getFields().size() == 5)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void givenExpression_whenExpressionEvaluationApplied_thenReturnCorrectValue() {
        // Given
        var expression = "Year";
        var csvResolver = csvResolverFactory.apply(LSOURCE.getSource());
        var sourceFlux = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(SOURCE), new TypeRef<>() {});
        var recordFlux = sourceFlux.apply(resolvedSource);
        var sourceRecord = recordFlux.blockFirst().getSourceRecord();
        var evaluationFactory = csvResolver.getExpressionEvaluationFactory();
        var evaluation = evaluationFactory.apply(sourceRecord);

        // When
        var evaluationResult = evaluation.apply(expression);

        // Then
        assertThat(evaluationResult.isPresent(), is(true));
        assertThat(evaluationResult.get(), is("1997"));
    }

    @Test
    void givenNonExistentColumnReference_whenExpressionEvaluationApplied_thenReturnEmpty() {
        // Given
        var csvResolver = csvResolverFactory.apply(LSOURCE.getSource());
        var sourceFlux = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(SOURCE), new TypeRef<>() {});
        var sourceRecord = sourceFlux.apply(resolvedSource).blockFirst().getSourceRecord();
        var evaluation = csvResolver.getExpressionEvaluationFactory().apply(sourceRecord);

        // When
        var evaluationResult = evaluation.apply("DoesNotExist");

        // Then
        assertThat(evaluationResult.isPresent(), is(false));
    }

    @Test
    void givenLargeColumns_whenRecordResolverApplied_thenReturnFluxWithLargeColumnRecords() throws IOException {
        var csv = IOUtils.toString(
                Objects.requireNonNull(CsvResolverTest.class.getResourceAsStream("large_column.csv")),
                StandardCharsets.UTF_8);
        var logicalSource = CarmlLogicalSource.builder()
                .source(RML_SOURCE)
                .referenceFormulation(CSV)
                .build();
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(csv), new TypeRef<>() {});
        var csvResolver = csvResolverFactory.apply(RML_SOURCE);
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(logicalSource));

        // When
        var recordFlux = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(recordFlux).expectNextCount(1).verifyComplete();
    }

    @Test
    void getInlineRecordParser_givenCsvWithHeader_returnsDataRows() {
        var csvResolver = csvResolverFactory.apply(RML_SOURCE);
        var parser = csvResolver.getInlineRecordParser().orElseThrow();

        var result = parser.apply("name,age\nalice,30\nbob,25");

        assertThat(result.size(), is(2));
        assertThat(result.get(0).getField("name"), is("alice"));
        assertThat(result.get(0).getField("age"), is("30"));
        assertThat(result.get(1).getField("name"), is("bob"));
        assertThat(result.get(1).getField("age"), is("25"));
    }

    @Test
    void getInlineRecordParser_givenSingleRowCsv_returnsSingleRecord() {
        var csvResolver = csvResolverFactory.apply(RML_SOURCE);
        var parser = csvResolver.getInlineRecordParser().orElseThrow();

        var result = parser.apply("item,price\nsword,1500");

        assertThat(result.size(), is(1));
        assertThat(result.get(0).getField("item"), is("sword"));
        assertThat(result.get(0).getField("price"), is("1500"));
    }

    @Test
    void givenFewerFieldsThanHeader_whenStrictFieldCount_thenError() {
        // Given
        var csv = "id,name,age\n6,Phoebe Buffay\n";
        var csvResolver = csvResolverFactory.apply(RML_SOURCE);
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(csv), new TypeRef<>() {});

        // When / Then
        StepVerifier.create(recordResolver.apply(resolvedSource))
                .expectErrorMatches(e -> e instanceof LogicalSourceResolverException
                        && e.getMessage().contains("has 2 field(s), but the header has 3"))
                .verify();
    }

    @Test
    void givenMoreFieldsThanHeader_whenStrictFieldCount_thenError() {
        // Given
        var csv = "id,name\n1,Alice,extra\n";
        var csvResolver = csvResolverFactory.apply(RML_SOURCE);
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(csv), new TypeRef<>() {});

        // When / Then
        StepVerifier.create(recordResolver.apply(resolvedSource))
                .expectErrorMatches(e -> e instanceof LogicalSourceResolverException
                        && e.getMessage().contains("has 3 field(s), but the header has 2"))
                .verify();
    }

    @Test
    void givenFewerFieldsThanHeader_whenStrictFieldCountDisabled_thenRecordEmitted() {
        // Given
        var csv = "id,name,age\n6,Phoebe Buffay\n";
        var options = CsvResolver.Options.builder().strictFieldCount(false).build();
        var csvResolver = CsvResolver.factory(options).apply(RML_SOURCE);
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(csv), new TypeRef<>() {});

        // When / Then
        StepVerifier.create(recordResolver.apply(resolvedSource))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void givenCustomCsvReaderBuilderFactory_whenApplied_thenCustomFactoryIsUsed() {
        // Given
        var customFactoryUsed = new AtomicBoolean(false);
        var options = CsvResolver.Options.builder()
                .csvReaderBuilderFactory(() -> {
                    customFactoryUsed.set(true);
                    return CsvReader.builder();
                })
                .build();
        var csvResolver = CsvResolver.factory(options).apply(RML_SOURCE);
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(SOURCE), new TypeRef<>() {});

        // When
        recordResolver.apply(resolvedSource).blockLast();

        // Then
        assertThat(customFactoryUsed.get(), is(true));
    }

    @Test
    void getInlineRecordParser_givenFewerFieldsThanHeader_whenStrictFieldCount_thenError() {
        // Given
        var csvResolver = csvResolverFactory.apply(RML_SOURCE);
        var parser = csvResolver.getInlineRecordParser().orElseThrow();

        // When / Then
        var exception = assertThrows(LogicalSourceResolverException.class, () -> parser.apply("id,name,age\n6,Phoebe"));
        assertThat(exception.getMessage(), containsString("has 2 field(s), but the header has 3"));
    }

    @Test
    void givenDuplicateHeaders_whenAllowDuplicateHeaders_thenRecordEmitted() {
        // Given
        var csv = "id,name,id\n1,Alice,2\n";
        var options = CsvResolver.Options.builder().allowDuplicateHeaders(true).build();
        var csvResolver = CsvResolver.factory(options).apply(RML_SOURCE);
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(csv), new TypeRef<>() {});

        // When / Then
        StepVerifier.create(recordResolver.apply(resolvedSource))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void givenDuplicateHeaders_whenDefaultOptions_thenError() {
        // Given
        var csv = "id,name,id\n1,Alice,2\n";
        var csvResolver = csvResolverFactory.apply(RML_SOURCE);
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(csv), new TypeRef<>() {});

        // When / Then
        StepVerifier.create(recordResolver.apply(resolvedSource))
                .expectError(CsvParseException.class)
                .verify();
    }
}
