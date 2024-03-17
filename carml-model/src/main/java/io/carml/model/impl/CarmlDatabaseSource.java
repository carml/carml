package io.carml.model.impl;

import io.carml.model.DatabaseSource;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.D2rq;
import io.carml.vocab.OldRml;
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
public class CarmlDatabaseSource extends CarmlResource implements DatabaseSource {

    private String jdbcDsn;

    private String jdbcDriver;

    private String username;

    @ToString.Exclude
    private CharSequence password;

    private String query;

    @RdfProperty(D2rq.jdbcDSN)
    @Override
    public String getJdbcDsn() {
        return jdbcDsn;
    }

    @RdfProperty(D2rq.jdbcDriver)
    @Override
    public String getJdbcDriver() {
        return jdbcDriver;
    }

    @RdfProperty(D2rq.username)
    @Override
    public String getUsername() {
        return username;
    }

    @RdfProperty(D2rq.password)
    @Override
    public CharSequence getPassword() {
        return password;
    }

    @RdfProperty(OldRml.query)
    @Override
    public String getQuery() {
        return query;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcDsn, jdbcDriver, username, password, query);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DatabaseSource other) {
            return Objects.equals(jdbcDsn, other.getJdbcDsn())
                    && Objects.equals(jdbcDriver, other.getJdbcDriver())
                    && Objects.equals(username, other.getUsername())
                    && Objects.equals(password, other.getPassword())
                    && Objects.equals(query, other.getQuery());
        }
        return false;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.D2rq.Database);

        if (jdbcDsn != null) {
            modelBuilder.add(D2rq.jdbcDSN, jdbcDsn);
        }

        if (jdbcDriver != null) {
            modelBuilder.add(D2rq.jdbcDriver, jdbcDriver);
        }

        if (username != null) {
            modelBuilder.add(D2rq.username, username);
        }

        if (password != null) {
            modelBuilder.add(D2rq.password, "REDACTED");
        }

        if (query != null) {
            modelBuilder.add(OldRml.query, query);
        }
    }
}
