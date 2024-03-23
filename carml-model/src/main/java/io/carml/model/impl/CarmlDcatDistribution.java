package io.carml.model.impl;

import io.carml.model.DcatDistribution;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import java.util.Objects;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.DCAT;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
public class CarmlDcatDistribution extends CarmlSource implements DcatDistribution {

    private IRI accessUrl;

    private IRI downloadUrl;

    @RdfProperty(DCAT.NAMESPACE + "accessURL")
    @Override
    public IRI getAccessUrl() {
        return accessUrl;
    }

    @RdfProperty(DCAT.NAMESPACE + "downloadURL")
    @Override
    public IRI getDownloadUrl() {
        return downloadUrl;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), accessUrl, downloadUrl);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DcatDistribution other) {
            return super.equalsSource(other)
                    && Objects.equals(accessUrl, other.getAccessUrl())
                    && Objects.equals(downloadUrl, other.getDownloadUrl());
        }
        return false;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, DCAT.DISTRIBUTION);
        super.addTriplesBase(modelBuilder);

        if (accessUrl != null) {
            modelBuilder.add(DCAT.ACCESS_URL, accessUrl);
        }
        if (downloadUrl != null) {
            modelBuilder.add(DCAT.DOWNLOAD_URL, downloadUrl);
        }
    }
}
