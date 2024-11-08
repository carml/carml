package io.carml.model;

import io.carml.model.impl.CarmlResource;
import io.carml.vocab.Rdf.Ql;
import io.carml.vocab.Rdf.Rml;
import io.carml.vocab.Rdf.Rr;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class SqlReferenceFormulation extends CarmlResource implements ReferenceFormulation {

    public static final Set<IRI> IRIS = Set.of(Rml.SQL2008Table, Rml.SQL2008Query, Ql.Rdb, Rr.SQL2008);

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {}
}
