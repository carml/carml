package io.carml.model;

import io.carml.model.impl.CarmlResource;
import io.carml.vocab.Rdf.Ql;
import io.carml.vocab.Rdf.Rml;
import java.util.Optional;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class JsonPathReferenceFormulation extends CarmlResource implements ReferenceFormulation {

    public static final Set<org.eclipse.rdf4j.model.Resource> IRIS = Set.of(Rml.JsonPath, Ql.JsonPath);

    /**
     * The JSONPath spec default iterator: {@code $} (the root JSON node). Per
     * {@code rml-io/spec/section/source-vocabulary.md}, a JSON source whose iterator is omitted
     * defaults to the document root.
     */
    private static final Object DEFAULT_ITERATOR = "$";

    @Override
    public Optional<Object> getDefaultIterator() {
        return Optional.of(DEFAULT_ITERATOR);
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {}
}
