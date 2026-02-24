package io.carml.logicalsourceresolver.sourceresolver.aspects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.carml.logicalsourceresolver.sourceresolver.PathRelativeTo;
import io.carml.model.impl.CarmlFilePath;
import io.carml.model.impl.source.csvw.CarmlCsvwTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CsvwSourceAspectsTest {

    private CsvwSourceAspects aspects;

    @BeforeEach
    void init() {
        aspects = new CsvwSourceAspects();
    }

    @Test
    void givenCsvwTable_whenSupportsSourceCalled_thenReturnTrue() {
        var csvwTable = CarmlCsvwTable.builder().build();
        assertThat(aspects.supportsSource(csvwTable), is(true));
    }

    @Test
    void givenNonCsvwSource_whenSupportsSourceCalled_thenReturnFalse() {
        var filePath = CarmlFilePath.of("foo.csv");
        assertThat(aspects.supportsSource(filePath), is(false));
    }

    @Test
    void givenAbsoluteUrl_whenGetUrlCalled_thenReturnUrl() {
        var csvwTable =
                CarmlCsvwTable.builder().url("http://example.org/data.csv").build();
        var urlFunction = aspects.getUrl().orElseThrow();
        var result = urlFunction.apply(csvwTable);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get().toString(), is("http://example.org/data.csv"));
    }

    @Test
    void givenAbsoluteUrl_whenGetPathStringCalled_thenReturnEmpty() {
        var csvwTable =
                CarmlCsvwTable.builder().url("http://example.org/data.csv").build();
        var pathStringFunction = aspects.getPathString().orElseThrow();
        var result = pathStringFunction.apply(csvwTable);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void givenRelativeUrl_whenGetUrlCalled_thenReturnEmpty() {
        var csvwTable = CarmlCsvwTable.builder().url("data.csv").build();
        var urlFunction = aspects.getUrl().orElseThrow();
        var result = urlFunction.apply(csvwTable);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void givenRelativeUrl_whenGetPathStringCalled_thenReturnPathString() {
        var csvwTable = CarmlCsvwTable.builder().url("data.csv").build();
        var pathStringFunction = aspects.getPathString().orElseThrow();
        var result = pathStringFunction.apply(csvwTable);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get(), is("data.csv"));
    }

    @Test
    void givenCsvwTable_whenGetPathRelativeToCalled_thenReturnMappingDirectory() {
        var csvwTable = CarmlCsvwTable.builder().build();
        var pathRelativeToFunction = aspects.getPathRelativeTo().orElseThrow();
        var result = pathRelativeToFunction.apply(csvwTable);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get(), is(PathRelativeTo.MAPPING_DIRECTORY));
    }

    @Test
    void givenNullUrl_whenGetUrlCalled_thenReturnEmpty() {
        var csvwTable = CarmlCsvwTable.builder().build();
        var urlFunction = aspects.getUrl().orElseThrow();
        var result = urlFunction.apply(csvwTable);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void givenNullUrl_whenGetPathStringCalled_thenReturnEmpty() {
        var csvwTable = CarmlCsvwTable.builder().build();
        var pathStringFunction = aspects.getPathString().orElseThrow();
        var result = pathStringFunction.apply(csvwTable);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void givenNonCsvwSource_whenGetPathRelativeToCalled_thenReturnEmpty() {
        var filePath = CarmlFilePath.of("foo.csv");
        var pathRelativeToFunction = aspects.getPathRelativeTo().orElseThrow();
        var result = pathRelativeToFunction.apply(filePath);

        assertThat(result.isPresent(), is(false));
    }
}
