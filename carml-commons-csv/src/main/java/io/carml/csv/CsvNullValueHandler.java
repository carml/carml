package io.carml.csv;

import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwTable;
import java.util.HashSet;
import java.util.Set;
import lombok.experimental.UtilityClass;

/**
 * Merges null-value definitions from RML ({@link Source#getNulls()}) and CSVW
 * ({@link CsvwTable#getCsvwNulls()}) into a unified, immutable set.
 *
 * <p>Both the reactive CSV resolver and the DuckDB CSV handler need to determine which field
 * values should be treated as null. This utility centralizes that merging logic.
 */
@UtilityClass
public class CsvNullValueHandler {

    /**
     * Resolves the effective set of null values for a given source.
     *
     * <p>If the source is a {@link CsvwTable}, its CSVW null values are merged with the RML null
     * values. Otherwise, only the RML null values are returned.
     *
     * @param source the data source
     * @return an immutable set of values that should be treated as null; never {@code null}
     */
    public static Set<Object> resolveNullValues(Source source) {
        var rmlNulls = source.getNulls();

        if (source instanceof CsvwTable csvwTable) {
            var csvwNulls = csvwTable.getCsvwNulls();
            if (csvwNulls != null && !csvwNulls.isEmpty()) {
                var merged = new HashSet<>(rmlNulls != null ? rmlNulls : Set.of());
                merged.addAll(csvwNulls);
                return Set.copyOf(merged);
            }
        }

        return rmlNulls != null ? rmlNulls : Set.of();
    }
}
