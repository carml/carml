package io.carml.logicalsourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver.LogicalSourceResolverFactory;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.model.impl.CarmlRelativePathSource;
import io.carml.util.TypeRef;
import io.carml.vocab.Rdf.Ql;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class CsvResolverTest {

    private static final Source RML_SOURCE = CarmlRelativePathSource.of("foo");

    private static final String SOURCE =
            """
                    Year,Make,Model,Description,Price
                    1997,Ford,E350,"ac, abs, moon",3000.00
                    1999,Chevy,"Venture ""Extended Edition""\","",4900.00""";

    private static final LogicalSource LSOURCE = CarmlLogicalSource.builder()
            .source(RML_SOURCE)
            .referenceFormulation(Ql.Csv)
            .build();

    private static final String SOURCE_DELIM =
            """
                    Year^Make^Model^Description^Price
                    1997^Ford^E350^"ac, abs, moon"^3000.00
                    1999^Chevy^"Venture ""Extended Edition""\"^""^4900.00""";

    private static final LogicalSource LSOURCE_DELIM = CarmlLogicalSource.builder()
            .source(RML_SOURCE)
            .referenceFormulation(Ql.Csv)
            .build();

    private final Function<Object, Mono<InputStream>> sourceResolver =
            s -> Mono.just(IOUtils.toInputStream((String) s, StandardCharsets.UTF_8));

    private LogicalSourceResolverFactory<NamedCsvRecord> csvResolverFactory;

    @BeforeEach
    public void init() {
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

    // @Test
    // void givenRecordFluxWithProvidedRecord_whenRecordResolverApplied_thenReturnFluxOfRecord() {
    //     // Given
    //     var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
    //     var namedCsvRecord = new NamedCsvRedcord(List.of("foo", "bar"));
    //     var record = new DefaultContext(1).toRecord(List.of("foo", "bar").toArray(String[]::new));
    //     var resolvedSource = ResolvedSource.of(RML_SOURCE, record, Record.class);
    //
    //     // When
    //     var records = recordResolver.apply(resolvedSource);
    //
    //     // Then
    //     StepVerifier.create(records)
    //             .expectNextMatches(logicalSourceRecord ->
    //                     logicalSourceRecord.getSourceRecord().equals(record))
    //             .verifyComplete();
    // }

    @Test
    void givenExpression_whenExpressionEvaluationApplied_thenReturnCorrectValue() {
        // Given
        var expression = "Year";
        var csvResolver = csvResolverFactory.apply(LSOURCE.getSource());
        var sourceFlux = csvResolver.getLogicalSourceRecords(Set.of(LSOURCE));
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(SOURCE), new TypeRef<>() {});
        var recordFlux = sourceFlux.apply(resolvedSource);
        var record = recordFlux.blockFirst().getSourceRecord();
        var evaluationFactory = csvResolver.getExpressionEvaluationFactory();
        var evaluation = evaluationFactory.apply(record);

        // When
        var evaluationResult = evaluation.apply(expression);

        // Then
        assertThat(evaluationResult.isPresent(), is(true));
        assertThat(evaluationResult.get(), is("1997"));
    }

    @Test
    void givenLargeColumns_whenRecordResolverApplied_thenReturnFluxWithLargeColumnRecords() throws IOException {
        var csv = IOUtils.toString(
                Objects.requireNonNull(CsvResolverTest.class.getResourceAsStream("large_column.csv")),
                StandardCharsets.UTF_8);
        var logicalSource = CarmlLogicalSource.builder()
                .source(RML_SOURCE)
                .referenceFormulation(Ql.Csv)
                .build();
        var resolvedSource = ResolvedSource.of(sourceResolver.apply(csv), new TypeRef<>() {});
        var csvResolver = csvResolverFactory.apply(RML_SOURCE);
        var recordResolver = csvResolver.getLogicalSourceRecords(Set.of(logicalSource));

        // When
        var recordFlux = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(recordFlux).expectNextCount(1).verifyComplete();
    }
}
