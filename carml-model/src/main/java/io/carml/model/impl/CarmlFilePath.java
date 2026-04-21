package io.carml.model.impl;

import io.carml.model.FilePath;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.util.Objects;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
public class CarmlFilePath extends CarmlSource implements FilePath {

    public static CarmlFilePath of(String path) {
        return CarmlFilePath.builder().path(path).build();
    }

    private Value root;

    private String path;

    @RdfProperty(Rml.root)
    @Override
    public Value getRoot() {
        return root;
    }

    @RdfProperty(Rml.path)
    @Override
    public String getPath() {
        return path;
    }

    /**
     * {@code rml:FilePath} does not carry a serialization property itself — per the RML-IO
     * specification, {@code rml:serialization} is a property of {@code rml:LogicalTarget}. This
     * method exists to satisfy the {@link io.carml.model.Target} contract (inherited via
     * {@link FilePath}) and always returns {@code null}. Callers reading an effective serialization
     * must consult the enclosing {@code rml:LogicalTarget} first.
     */
    @Override
    public IRI getSerialization() {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), root, path);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FilePath other) {
            return super.equalsSource(other)
                    && Objects.equals(root, other.getRoot())
                    && Objects.equals(path, other.getPath());
        }

        return false;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.FilePath);
        super.addTriplesBase(modelBuilder);

        if (root != null) {
            modelBuilder.add(Rdf.Rml.root, String.valueOf(root));
        }

        if (path != null) {
            modelBuilder.add(Rdf.Rml.path, path);
        }
    }
}
