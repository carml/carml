package io.carml.logicalview.duckdb;

import io.carml.vocab.Rdf;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.eclipse.rdf4j.model.Resource;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.SelectField;
import org.jooq.Table;

/**
 * Encapsulates formulation-specific SQL compilation for mixed-formulation iterable fields. Each
 * implementation handles one target reference formulation (e.g., JSONPath, CSV) and knows how to
 * produce the UNNEST table, nested field extractions, and type companions for that formulation when
 * the data is embedded as a string in a parent field of a different formulation.
 *
 * <p>To add support for a new reference formulation in mixed-formulation contexts, implement this
 * interface and register a factory entry in {@link #FACTORIES}.
 */
interface MixedFormulationCompiler {

    /**
     * Registry of compiler factories keyed by reference formulation IRI. Each entry maps a
     * formulation IRI to a factory function that creates a compiler bound to a parent value field.
     */
    Map<Resource, Function<Field<?>, MixedFormulationCompiler>> FACTORIES = Map.of(
            Rdf.Rml.JsonPath, JsonChildCompiler::new,
            Rdf.Ql.JsonPath, JsonChildCompiler::new,
            Rdf.Rml.Csv, CsvChildCompiler::new,
            Rdf.Ql.Csv, CsvChildCompiler::new);

    Table<?> compileUnnestTable(String iterator, String absoluteName);

    SelectField<?> compileNestedField(String unnestAlias, String reference, Name fieldAlias);

    SelectField<?> compileNestedFieldType(String unnestAlias, String reference, Name typeAlias);

    /**
     * Finds a compiler for the given reference formulation IRI and binds it to the parent value.
     *
     * @param formulation the child's reference formulation IRI
     * @param parentValueField the jOOQ field expression for the parent value
     * @return the bound compiler, or empty if no compiler supports this formulation
     */
    static Optional<MixedFormulationCompiler> forFormulation(Resource formulation, Field<?> parentValueField) {
        return Optional.ofNullable(FACTORIES.get(formulation)).map(factory -> factory.apply(parentValueField));
    }
}
