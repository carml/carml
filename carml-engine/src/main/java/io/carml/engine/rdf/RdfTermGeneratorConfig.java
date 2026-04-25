package io.carml.engine.rdf;

import io.carml.functions.FunctionRegistry;
import java.text.Normalizer;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(toBuilder = true)
@Getter
public class RdfTermGeneratorConfig {

    @NonNull
    private IRI baseIri;

    @NonNull
    private ValueFactory valueFactory;

    @NonNull
    private final Normalizer.Form normalizationForm;

    private final boolean iriUpperCasePercentEncoding;

    private final FunctionRegistry functionRegistry;

    @Default
    private final Set<String> iriSafeFieldNames = Set.of();
}
