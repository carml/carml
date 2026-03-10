package io.carml.logicalview.duckdb;

import io.carml.model.Field;
import io.carml.model.LogicalSource;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;

/**
 * Encapsulates the logic for reading a specific kind of data source in DuckDB. Each
 * implementation handles one formulation family (JSON, CSV, SQL), providing both
 * compatibility checking (for the factory) and source compilation (for the compiler).
 *
 * <p>This creates a single extension point for adding new reference formulations:
 * implement this interface and add the handler to {@link #HANDLERS}.
 */
sealed interface DuckDbSourceHandler permits JsonPathSourceHandler, CsvSourceHandler, SqlSourceHandler {

    /**
     * Pairs the compiled source SQL with the {@link DuckDbSourceStrategy} determined during source
     * dispatch. This avoids duplicating source-type detection between source clause compilation and
     * strategy selection.
     *
     * @param sourceSql the SQL expression for the CTE source (e.g., {@code read_json_auto(...)})
     * @param strategy the field access strategy matching the source type
     */
    record CompiledSource(String sourceSql, DuckDbSourceStrategy strategy) {}

    /**
     * Returns the set of reference formulation IRIs that this handler supports.
     */
    Set<Resource> supportedFormulations();

    /**
     * Checks whether the given logical source is compatible with this handler. This goes beyond
     * formulation matching — for example, JSON sources must be file-based and must not use
     * recursive descent.
     *
     * @param logicalSource the logical source to check
     * @return {@code true} if this handler can process the source
     */
    boolean isCompatible(LogicalSource logicalSource);

    /**
     * Compiles the logical source into a DuckDB SQL source expression and selects the appropriate
     * field access strategy.
     *
     * @param logicalSource the logical source to compile
     * @param viewFields the fields declared on the logical view (used for JSON field mapping)
     * @param cteAlias the alias for the CTE that wraps the source expression
     * @return the compiled source SQL and strategy
     */
    CompiledSource compileSource(LogicalSource logicalSource, Set<Field> viewFields, String cteAlias);

    /**
     * All registered handlers, in dispatch order.
     */
    List<DuckDbSourceHandler> HANDLERS =
            List.of(new JsonPathSourceHandler(), new CsvSourceHandler(), new SqlSourceHandler());

    /**
     * Finds the handler for a given reference formulation IRI.
     *
     * @param refFormulationIri the reference formulation IRI
     * @return the handler, or empty if no handler supports this formulation
     */
    static Optional<DuckDbSourceHandler> forFormulation(Resource refFormulationIri) {
        return HANDLERS.stream()
                .filter(handler -> handler.supportedFormulations().contains(refFormulationIri))
                .findFirst();
    }
}
