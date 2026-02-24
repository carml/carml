package io.carml.logicalsourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver.LogicalSourceResolverFactory;
import io.carml.model.CsvReferenceFormulation;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.model.impl.source.csvw.CarmlCsvwDialect;
import io.carml.model.impl.source.csvw.CarmlCsvwTable;
import io.carml.util.TypeRef;
import io.carml.vocab.Rdf.Rml;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class CsvResolverCsvwTest {

    private static final CsvReferenceFormulation CSV =
            CsvReferenceFormulation.builder().id(Rml.Csv.stringValue()).build();

    private final Function<String, Mono<InputStream>> toInputStream =
            s -> Mono.just(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));

    private LogicalSourceResolverFactory<NamedCsvRecord> csvResolverFactory;

    @BeforeEach
    void init() {
        csvResolverFactory = CsvResolver.factory();
    }

    @Test
    void givenCsvwTrimEnabled_whenExpressionEvaluated_thenValueIsTrimmed() {
        // Given
        var csv = "name,age\n  Alice  , 30 \n";
        var dialect = CarmlCsvwDialect.builder().trim(true).build();
        var csvwTable = CarmlCsvwTable.builder().dialect(dialect).build();
        var logicalSource = CarmlLogicalSource.builder()
                .source(csvwTable)
                .referenceFormulation(CSV)
                .build();

        var csvResolver = csvResolverFactory.apply(csvwTable);
        var recordFlux = csvResolver
                .getLogicalSourceRecords(Set.of(logicalSource))
                .apply(ResolvedSource.of(toInputStream.apply(csv), new TypeRef<>() {}));
        var record = recordFlux.blockFirst().getSourceRecord();
        var evaluation = csvResolver.getExpressionEvaluationFactory().apply(record);

        // When
        var nameResult = evaluation.apply("name");
        var ageResult = evaluation.apply("age");

        // Then
        assertThat(nameResult.isPresent(), is(true));
        assertThat(nameResult.get(), is("Alice"));
        assertThat(ageResult.isPresent(), is(true));
        assertThat(ageResult.get(), is("30"));
    }

    @Test
    void givenCsvwTrimDisabled_whenExpressionEvaluated_thenValueIsNotTrimmed() {
        // Given
        var csv = "name,age\n  Alice  , 30 \n";
        var dialect = CarmlCsvwDialect.builder().trim(false).build();
        var csvwTable = CarmlCsvwTable.builder().dialect(dialect).build();
        var logicalSource = CarmlLogicalSource.builder()
                .source(csvwTable)
                .referenceFormulation(CSV)
                .build();

        var csvResolver = csvResolverFactory.apply(csvwTable);
        var recordFlux = csvResolver
                .getLogicalSourceRecords(Set.of(logicalSource))
                .apply(ResolvedSource.of(toInputStream.apply(csv), new TypeRef<>() {}));
        var record = recordFlux.blockFirst().getSourceRecord();
        var evaluation = csvResolver.getExpressionEvaluationFactory().apply(record);

        // When
        var nameResult = evaluation.apply("name");

        // Then
        assertThat(nameResult.isPresent(), is(true));
        assertThat(nameResult.get(), is("  Alice  "));
    }

    @Test
    void givenCsvwNullValues_whenFieldMatchesNull_thenReturnEmpty() {
        // Given
        var csv = "name,age\nNULL,30\n";
        var csvwTable = CarmlCsvwTable.builder().csvwNull("NULL").build();
        var logicalSource = CarmlLogicalSource.builder()
                .source(csvwTable)
                .referenceFormulation(CSV)
                .build();

        var csvResolver = csvResolverFactory.apply(csvwTable);
        var recordFlux = csvResolver
                .getLogicalSourceRecords(Set.of(logicalSource))
                .apply(ResolvedSource.of(toInputStream.apply(csv), new TypeRef<>() {}));
        var record = recordFlux.blockFirst().getSourceRecord();
        var evaluation = csvResolver.getExpressionEvaluationFactory().apply(record);

        // When
        var nameResult = evaluation.apply("name");
        var ageResult = evaluation.apply("age");

        // Then
        assertThat(nameResult.isPresent(), is(false));
        assertThat(ageResult.isPresent(), is(true));
        assertThat(ageResult.get(), is("30"));
    }

    @Test
    void givenCsvwAndRmlNullValues_whenFieldMatchesEither_thenReturnEmpty() {
        // Given
        var csv = "name,age,city\nNULL,N/A,Berlin\n";
        var csvwTable =
                CarmlCsvwTable.builder().csvwNull("NULL").nulls(Set.of("N/A")).build();
        var logicalSource = CarmlLogicalSource.builder()
                .source(csvwTable)
                .referenceFormulation(CSV)
                .build();

        var csvResolver = csvResolverFactory.apply(csvwTable);
        var recordFlux = csvResolver
                .getLogicalSourceRecords(Set.of(logicalSource))
                .apply(ResolvedSource.of(toInputStream.apply(csv), new TypeRef<>() {}));
        var record = recordFlux.blockFirst().getSourceRecord();
        var evaluation = csvResolver.getExpressionEvaluationFactory().apply(record);

        // When
        var nameResult = evaluation.apply("name");
        var ageResult = evaluation.apply("age");
        var cityResult = evaluation.apply("city");

        // Then
        assertThat(nameResult.isPresent(), is(false));
        assertThat(ageResult.isPresent(), is(false));
        assertThat(cityResult.isPresent(), is(true));
        assertThat(cityResult.get(), is("Berlin"));
    }

    @Test
    void givenCsvWithUtf8Bom_whenRecordResolverApplied_thenBomIsStripped() {
        // Given - UTF-8 BOM (EF BB BF) followed by CSV content
        var csvBytes = new byte[] {
            (byte) 0xEF,
            (byte) 0xBB,
            (byte) 0xBF, // UTF-8 BOM
            'n',
            'a',
            'm',
            'e',
            ',',
            'a',
            'g',
            'e',
            '\n',
            'A',
            'l',
            'i',
            'c',
            'e',
            ',',
            '3',
            '0',
            '\n'
        };
        var csvwTable = CarmlCsvwTable.builder().build();
        var logicalSource = CarmlLogicalSource.builder()
                .source(csvwTable)
                .referenceFormulation(CSV)
                .build();

        var csvResolver = csvResolverFactory.apply(csvwTable);
        var inputStream = Mono.just((InputStream) new ByteArrayInputStream(csvBytes));
        var recordFlux = csvResolver
                .getLogicalSourceRecords(Set.of(logicalSource))
                .apply(ResolvedSource.of(inputStream, new TypeRef<>() {}));
        var record = recordFlux.blockFirst().getSourceRecord();
        var evaluation = csvResolver.getExpressionEvaluationFactory().apply(record);

        // When - "name" header should be accessible without BOM prefix
        var nameResult = evaluation.apply("name");

        // Then
        assertThat(nameResult.isPresent(), is(true));
        assertThat(nameResult.get(), is("Alice"));
    }

    @Test
    void givenCsvwTrimAndNullValues_whenPaddedValueMatchesNullAfterTrim_thenReturnEmpty() {
        // Given - "  NULL  " should be trimmed to "NULL" which matches the null value
        var csv = "name,age\n  NULL  ,30\n";
        var dialect = CarmlCsvwDialect.builder().trim(true).build();
        var csvwTable =
                CarmlCsvwTable.builder().dialect(dialect).csvwNull("NULL").build();
        var logicalSource = CarmlLogicalSource.builder()
                .source(csvwTable)
                .referenceFormulation(CSV)
                .build();

        var csvResolver = csvResolverFactory.apply(csvwTable);
        var recordFlux = csvResolver
                .getLogicalSourceRecords(Set.of(logicalSource))
                .apply(ResolvedSource.of(toInputStream.apply(csv), new TypeRef<>() {}));
        var record = recordFlux.blockFirst().getSourceRecord();
        var evaluation = csvResolver.getExpressionEvaluationFactory().apply(record);

        // When
        var nameResult = evaluation.apply("name");

        // Then
        assertThat(nameResult.isPresent(), is(false));
    }

    @Test
    void givenCsvwWithNoDialect_whenRecordResolverApplied_thenDefaultsAreUsed() {
        // Given
        var csv = "name,age\nAlice,30\n";
        var csvwTable = CarmlCsvwTable.builder().build();
        var logicalSource = CarmlLogicalSource.builder()
                .source(csvwTable)
                .referenceFormulation(CSV)
                .build();

        var csvResolver = csvResolverFactory.apply(csvwTable);
        var recordFlux = csvResolver
                .getLogicalSourceRecords(Set.of(logicalSource))
                .apply(ResolvedSource.of(toInputStream.apply(csv), new TypeRef<>() {}));

        // When / Then
        StepVerifier.create(recordFlux).expectNextCount(1).verifyComplete();
    }
}
