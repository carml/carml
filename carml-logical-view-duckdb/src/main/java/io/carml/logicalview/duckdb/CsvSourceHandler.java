package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.inline;

import io.carml.model.Field;
import io.carml.model.LogicalSource;
import io.carml.vocab.Rdf;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;

/**
 * Handles CSV data sources.
 *
 * <p>Uses {@code read_csv_auto} for CSV files and {@code read_parquet} for Parquet files.
 * All fields are accessed via {@link ColumnSourceStrategy} (direct column references).
 */
@Slf4j
final class CsvSourceHandler implements DuckDbSourceHandler {

    private static final Set<Resource> SUPPORTED = Set.of(Rdf.Ql.Csv, Rdf.Rml.Csv);

    @Override
    public Set<Resource> supportedFormulations() {
        return SUPPORTED;
    }

    @Override
    public boolean isCompatible(LogicalSource logicalSource) {
        if (!DuckDbFileSourceUtils.isFileBasedSource(logicalSource.getSource())) {
            LOG.debug("LogicalSource has file-based reference formulation but source is not file-based");
            return false;
        }
        return true;
    }

    @Override
    public CompiledSource compileSource(LogicalSource logicalSource, Set<Field> viewFields, String cteAlias) {
        var filePath = DuckDbFileSourceUtils.resolveFilePath(logicalSource.getSource());

        if (DuckDbFileSourceUtils.isParquetFile(filePath)) {
            return columnSource("read_parquet(%s)".formatted(inline(filePath)), cteAlias);
        }

        return columnSource("read_csv_auto(%s)".formatted(inline(filePath)), cteAlias);
    }

    private static CompiledSource columnSource(String sourceSql, String cteAlias) {
        return new CompiledSource(sourceSql, new ColumnSourceStrategy(cteAlias, false));
    }
}
