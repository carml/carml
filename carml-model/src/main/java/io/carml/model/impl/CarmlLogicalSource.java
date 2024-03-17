package io.carml.model.impl;

import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.OldRml;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlLogicalSource extends CarmlResource implements LogicalSource {

    protected Object source;

    protected String iterator;

    protected IRI referenceFormulation;

    protected String tableName;

    protected String sqlQuery;

    protected IRI sqlVersion;

    private Set<String> expressions;

    @RdfProperty(value = Rml.source, handler = LogicalSourceSourcePropertyHandler.class)
    @RdfProperty(value = OldRml.source, handler = LogicalSourceSourcePropertyHandler.class)
    @Override
    public Object getSource() {
        if (source == null && hasTableNameOrSqlQuery()) {
            return CarmlDatabaseSource.builder()
                    .query(tableName != null ? String.format("SELECT * FROM %s", tableName) : sqlQuery)
                    .build();
        }

        return source;
    }

    private boolean hasTableNameOrSqlQuery() {
        return tableName != null || sqlQuery != null;
    }

    @RdfProperty(Rml.iterator)
    @RdfProperty(OldRml.iterator)
    @Override
    public String getIterator() {
        return iterator;
    }

    @RdfProperty(Rml.referenceFormulation)
    @RdfProperty(OldRml.referenceFormulation)
    @Override
    public IRI getReferenceFormulation() {
        if (source instanceof DatabaseSource || hasTableNameOrSqlQuery()) {
            return Rdf.Ql.Rdb;
        }

        return referenceFormulation;
    }

    @RdfProperty(Rr.tableName)
    @Override
    public String getTableName() {
        if (referenceFormulation != null && referenceFormulation.equals(Rdf.Rml.SQL2008Table)) {
            return iterator;
        }

        return tableName;
    }

    @RdfProperty(Rr.sqlQuery)
    @RdfProperty(OldRml.query)
    @Override
    public String getSqlQuery() {
        return sqlQuery;
    }

    @RdfProperty(Rr.sqlVersion)
    @Override
    public IRI getSqlVersion() {
        return sqlVersion;
    }

    @Override
    public String getQuery() {
        String query = null;

        if (referenceFormulation != null && referenceFormulation.equals(Rdf.Rml.SQL2008Query)) {
            query = iterator;
        }

        if (query == null && sqlQuery != null) {
            query = sqlQuery;
        }

        if (query == null
                && source instanceof DatabaseSource databaseSource
                && referenceFormulation != null
                && !referenceFormulation.equals(Rdf.Rml.SQL2008Table)) {
            query = databaseSource.getQuery();
        }

        if (query == null) {
            return null;
        }

        return trimTrailingSemiColon(query);
    }

    private String trimTrailingSemiColon(String in) {
        if (in.endsWith(";")) {
            return in.substring(0, in.length() - 1);
        }
        return in;
    }

    @Override
    public Set<String> getExpressions() {
        return expressions;
    }

    public Set<Resource> getReferencedResources() {
        if (source instanceof Resource resource) {
            return Set.of(resource);
        } else {
            return Set.of();
        }
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.LogicalSource);
        if (source != null) {
            if (source instanceof Resource resource) {
                modelBuilder.add(Rdf.Rml.source, resource.getAsResource());
            } else {
                modelBuilder.add(Rdf.Rml.source, source);
            }
        }
        if (iterator != null) {
            modelBuilder.add(Rdf.Rml.iterator, iterator);
        }
        if (referenceFormulation != null) {
            modelBuilder.add(Rdf.Rml.referenceFormulation, referenceFormulation);
        }
    }
}
