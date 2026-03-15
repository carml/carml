package io.carml.csv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import io.carml.model.source.csvw.CsvwDialect;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CsvwDialectProcessorTest {

    @Mock
    private CsvwDialect dialect;

    @Test
    void process_returnsDefaults_whenDialectHasNoOverrides() {
        // Given
        when(dialect.getDelimiter()).thenReturn(null);
        when(dialect.getQuoteChar()).thenReturn(null);
        when(dialect.getCommentPrefix()).thenReturn(null);
        when(dialect.getEncoding()).thenReturn(null);
        when(dialect.getSkipRows()).thenReturn(0);
        when(dialect.getDoubleQuote()).thenReturn(null);
        when(dialect.trim()).thenReturn(false);

        // When
        var config = CsvwDialectProcessor.process(dialect);

        // Then
        assertThat(config.delimiter(), is(Optional.empty()));
        assertThat(config.quoteChar(), is(Optional.empty()));
        assertThat(config.commentPrefix(), is(Optional.empty()));
        assertThat(config.encoding(), is(Optional.empty()));
        assertThat(config.skipRows(), is(0));
        assertThat(config.useDoubleQuoteEscaping(), is(true));
        assertThat(config.trim(), is(false));
    }

    @Test
    void process_extractsAllProperties_whenDialectIsFullyConfigured() {
        // Given
        when(dialect.getDelimiter()).thenReturn(";");
        when(dialect.getQuoteChar()).thenReturn("'");
        when(dialect.getCommentPrefix()).thenReturn("#");
        when(dialect.getEncoding()).thenReturn("UTF-16");
        when(dialect.getSkipRows()).thenReturn(2);
        when(dialect.getDoubleQuote()).thenReturn("false");
        when(dialect.trim()).thenReturn(true);

        // When
        var config = CsvwDialectProcessor.process(dialect);

        // Then
        assertThat(config.delimiter(), is(Optional.of(';')));
        assertThat(config.quoteChar(), is(Optional.of('\'')));
        assertThat(config.commentPrefix(), is(Optional.of('#')));
        assertThat(config.encoding(), is(Optional.of("UTF-16")));
        assertThat(config.skipRows(), is(2));
        assertThat(config.useDoubleQuoteEscaping(), is(false));
        assertThat(config.trim(), is(true));
    }

    @Test
    void process_throwsCsvProcessingException_whenDelimiterIsMultiChar() {
        // Given
        when(dialect.getDelimiter()).thenReturn(";;");

        // When / Then
        var exception = assertThrows(CsvProcessingException.class, () -> CsvwDialectProcessor.process(dialect));
        assertThat(exception.getMessage(), is("CSVW delimiter must be a single character, but was ';;'"));
    }

    @Test
    void process_throwsCsvProcessingException_whenQuoteCharIsMultiChar() {
        // Given
        when(dialect.getDelimiter()).thenReturn(null);
        when(dialect.getQuoteChar()).thenReturn("''");

        // When / Then
        var exception = assertThrows(CsvProcessingException.class, () -> CsvwDialectProcessor.process(dialect));
        assertThat(exception.getMessage(), is("CSVW quote character must be a single character, but was ''''"));
    }

    @Test
    void process_throwsCsvProcessingException_whenCommentPrefixIsMultiChar() {
        // Given
        when(dialect.getDelimiter()).thenReturn(null);
        when(dialect.getQuoteChar()).thenReturn(null);
        when(dialect.getCommentPrefix()).thenReturn("##");

        // When / Then
        var exception = assertThrows(CsvProcessingException.class, () -> CsvwDialectProcessor.process(dialect));
        assertThat(exception.getMessage(), is("CSVW comment prefix must be a single character, but was '##'"));
    }

    @Test
    void process_handlesEmptyStrings_asEmptyOptionals() {
        // Given
        when(dialect.getDelimiter()).thenReturn("");
        when(dialect.getQuoteChar()).thenReturn("");
        when(dialect.getCommentPrefix()).thenReturn("");
        when(dialect.getEncoding()).thenReturn(null);
        when(dialect.getSkipRows()).thenReturn(0);
        when(dialect.getDoubleQuote()).thenReturn(null);
        when(dialect.trim()).thenReturn(false);

        // When
        var config = CsvwDialectProcessor.process(dialect);

        // Then
        assertThat(config.delimiter(), is(Optional.empty()));
        assertThat(config.quoteChar(), is(Optional.empty()));
        assertThat(config.commentPrefix(), is(Optional.empty()));
    }

    @Test
    void process_doubleQuoteTrue_enablesDoubleQuoteEscaping() {
        // Given
        when(dialect.getDelimiter()).thenReturn(null);
        when(dialect.getQuoteChar()).thenReturn(null);
        when(dialect.getCommentPrefix()).thenReturn(null);
        when(dialect.getEncoding()).thenReturn(null);
        when(dialect.getSkipRows()).thenReturn(0);
        when(dialect.getDoubleQuote()).thenReturn("true");
        when(dialect.trim()).thenReturn(false);

        // When
        var config = CsvwDialectProcessor.process(dialect);

        // Then
        assertThat(config.useDoubleQuoteEscaping(), is(true));
    }

    @Test
    void toChar_returnsEmpty_whenInputIsNull() {
        assertThat(CsvwDialectProcessor.toChar(null, "test"), is(Optional.empty()));
    }

    @Test
    void toChar_returnsEmpty_whenInputIsEmpty() {
        assertThat(CsvwDialectProcessor.toChar("", "test"), is(Optional.empty()));
    }

    @Test
    void toChar_returnsCharacter_whenInputIsSingleChar() {
        assertThat(CsvwDialectProcessor.toChar(",", "test"), is(Optional.of(',')));
    }

    @Test
    void toChar_throwsCsvProcessingException_whenInputIsMultiChar() {
        var exception = assertThrows(CsvProcessingException.class, () -> CsvwDialectProcessor.toChar("ab", "test"));
        assertThat(exception.getMessage(), is("test must be a single character, but was 'ab'"));
    }
}
