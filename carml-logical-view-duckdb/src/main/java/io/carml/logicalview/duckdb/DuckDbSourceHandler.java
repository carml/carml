package io.carml.logicalview.duckdb;

import io.carml.model.Field;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.Mapping;
import java.sql.Connection;
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
     * Indicates whether this handler reads from file-based sources (so the file-existence validator
     * and the introspector should treat its sources as files). Default: {@code false}.
     */
    default boolean isFileBased() {
        return false;
    }

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
     * field access strategy. Implementations that need handler-specific compilation context (e.g.
     * the SQL handler's database attacher, the JSON handler's NDJSON transcode cache) read it via
     * package-private accessors on {@link DuckDbViewCompiler}, so this signature stays focused on
     * the source itself rather than accumulating cross-cutting parameters.
     *
     * @param logicalSource the logical source to compile
     * @param viewFields the fields declared on the logical view (used for JSON field mapping)
     * @param cteAlias the alias for the CTE that wraps the source expression
     * @return the compiled source SQL and strategy
     */
    CompiledSource compileSource(LogicalSource logicalSource, Set<Field> viewFields, String cteAlias);

    /**
     * Validates the view's source data against the view's field references. Implementations may
     * perform formulation-specific checks such as verifying that CSV column references match the
     * actual file headers case-sensitively.
     *
     * <p>The default implementation performs no validation. Override in handlers that require
     * source-level validation before compilation.
     *
     * @param view the logical view to validate
     * @param connection the DuckDB JDBC connection for querying source metadata
     */
    default void validate(LogicalView view, Connection connection) {
        // No-op by default
    }

    /**
     * Resolves the effective file path or URL string for {@code logicalSource}. Returns the
     * path that DuckDB will read from for file-based sources. The default implementation handles
     * the formulation-agnostic source shapes ({@link io.carml.model.FilePath},
     * {@link io.carml.model.FileSource}); handlers whose formulation introduces additional source
     * types (e.g. CSV with {@code csvw:Table}) override this method to resolve the formulation-
     * specific shapes themselves and fall through to the default for the rest.
     *
     * @param logicalSource the logical source whose source path to resolve
     * @param mapping the active mapping context for {@code rml:MappingDirectory} anchoring; may be
     *     {@code null} when no mapping is bound (test harness, introspector)
     * @return the effective file path or URL string
     * @throws IllegalArgumentException if the source is not file-based or is otherwise unsupported
     *     by this handler
     */
    default String resolveFilePath(LogicalSource logicalSource, Mapping mapping) {
        return DuckDbFileSourceUtils.resolveFilePath(logicalSource.getSource(), mapping);
    }

    /**
     * Convenience that resolves the file path for the matching file-based handler (if any). Used
     * by callers that walk source trees without knowing the reference formulation up-front (e.g.
     * the file-existence validator, the introspector). Returns empty when no handler supports the
     * formulation, or when the matched handler is not file-based.
     */
    static Optional<String> resolveFilePathFor(LogicalSource logicalSource, Mapping mapping) {
        var refFormulation = logicalSource.getReferenceFormulation();
        if (refFormulation == null) {
            return Optional.empty();
        }
        return forFormulation(refFormulation.getAsResource())
                .filter(DuckDbSourceHandler::isFileBased)
                .map(handler -> handler.resolveFilePath(logicalSource, mapping));
    }

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
