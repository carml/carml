package io.carml.engine.rdf;

import io.carml.engine.TermGeneratorFactory;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(toBuilder = true)
@Getter
public class RdfMapperConfig {

    @NonNull
    private final Supplier<ValueFactory> valueFactorySupplier;

    @Getter(AccessLevel.NONE)
    private final TermGeneratorFactory<Value> termGeneratorFactory;

    @NonNull
    private final RdfTermGeneratorConfig rdfTermGeneratorConfig;

    public TermGeneratorFactory<Value> getTermGeneratorFactory() {
        return termGeneratorFactory != null ? termGeneratorFactory : RdfTermGeneratorFactory.of(rdfTermGeneratorConfig);
    }

    @Builder.Default
    private final boolean strictMode = false;

    @Builder.Default
    private final boolean allowMultipleSubjectMaps = false;
}
