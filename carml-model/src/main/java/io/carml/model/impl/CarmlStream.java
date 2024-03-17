package io.carml.model.impl;

import io.carml.model.NameableStream;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Carml;
import io.carml.vocab.Rdf;
import java.util.Objects;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
public class CarmlStream extends CarmlResource implements NameableStream {

    private String streamName;

    @RdfProperty(Carml.streamName)
    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NameableStream other) {
            return Objects.equals(streamName, other.getStreamName());
        }
        return false;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Carml.Stream);

        if (streamName != null) {
            modelBuilder.add(Carml.streamName, streamName);
        }
    }
}
