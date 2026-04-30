package io.carml.model;

import java.util.Optional;
import org.eclipse.rdf4j.model.IRI;

public interface LogicalSource extends AbstractLogicalSource {

    Source getSource();

    String getIterator();

    ReferenceFormulation getReferenceFormulation();

    String getTableName();

    String getSqlQuery();

    IRI getSqlVersion();

    String getQuery();

    /**
     * Resolves the effective iterator for this logical source. Returns the declared
     * {@link #getIterator() iterator} when set; otherwise falls back to the reference
     * formulation's {@link ReferenceFormulation#getDefaultIterator() default iterator} per
     * {@code rml-io/spec/section/source-vocabulary.md}. Returns empty when no iterator is
     * declared and the formulation defines no default (e.g. CSV row-based, SQL table).
     */
    @Override
    default Optional<Object> resolveIterator() {
        var declared = getIterator();
        if (declared != null && !declared.isBlank()) {
            return Optional.of(declared);
        }
        var formulation = getReferenceFormulation();
        return formulation == null ? Optional.empty() : formulation.getDefaultIterator();
    }
}
