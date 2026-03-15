package io.carml.csv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwTable;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CsvNullValueHandlerTest {

    @Mock
    private Source source;

    @Mock
    private CsvwTable csvwTable;

    @Test
    void resolveNullValues_returnsEmptySet_whenSourceHasNoNulls() {
        // Given
        when(source.getNulls()).thenReturn(null);

        // When
        var result = CsvNullValueHandler.resolveNullValues(source);

        // Then
        assertThat(result, is(empty()));
    }

    @Test
    void resolveNullValues_returnsRmlNulls_whenSourceIsNotCsvwTable() {
        // Given
        when(source.getNulls()).thenReturn(Set.of("N/A", "NULL"));

        // When
        var result = CsvNullValueHandler.resolveNullValues(source);

        // Then
        assertThat(result, containsInAnyOrder("N/A", "NULL"));
    }

    @Test
    void resolveNullValues_returnsCsvwNulls_whenOnlyCsvwNullsExist() {
        // Given
        when(csvwTable.getNulls()).thenReturn(null);
        when(csvwTable.getCsvwNulls()).thenReturn(Set.of("NULL"));

        // When
        var result = CsvNullValueHandler.resolveNullValues(csvwTable);

        // Then
        assertThat(result, containsInAnyOrder((Object) "NULL"));
    }

    @Test
    void resolveNullValues_mergesRmlAndCsvwNulls_whenBothExist() {
        // Given
        when(csvwTable.getNulls()).thenReturn(Set.of("N/A"));
        when(csvwTable.getCsvwNulls()).thenReturn(Set.of("NULL"));

        // When
        var result = CsvNullValueHandler.resolveNullValues(csvwTable);

        // Then
        assertThat(result, containsInAnyOrder((Object) "N/A", "NULL"));
    }

    @Test
    void resolveNullValues_returnsRmlNulls_whenCsvwNullsAreEmpty() {
        // Given
        when(csvwTable.getNulls()).thenReturn(Set.of("N/A"));
        when(csvwTable.getCsvwNulls()).thenReturn(Set.of());

        // When
        var result = CsvNullValueHandler.resolveNullValues(csvwTable);

        // Then
        assertThat(result, containsInAnyOrder((Object) "N/A"));
    }

    @Test
    void resolveNullValues_returnsRmlNulls_whenCsvwNullsAreNull() {
        // Given
        when(csvwTable.getNulls()).thenReturn(Set.of("N/A"));
        when(csvwTable.getCsvwNulls()).thenReturn(null);

        // When
        var result = CsvNullValueHandler.resolveNullValues(csvwTable);

        // Then
        assertThat(result, containsInAnyOrder((Object) "N/A"));
    }

    @Test
    void resolveNullValues_returnsEmptySet_whenCsvwTableHasNoNulls() {
        // Given
        when(csvwTable.getNulls()).thenReturn(null);
        when(csvwTable.getCsvwNulls()).thenReturn(null);

        // When
        var result = CsvNullValueHandler.resolveNullValues(csvwTable);

        // Then
        assertThat(result, is(empty()));
    }
}
