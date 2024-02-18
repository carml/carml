package io.carml.model.impl;

import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
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
  @Override
  public String getIterator() {
    return iterator;
  }

  @RdfProperty(Rml.referenceFormulation)
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
    return tableName;
  }

  @RdfProperty(Rr.sqlQuery)
  @RdfProperty(Rml.query)
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
    if (sqlQuery != null) {
      query = sqlQuery;
    }

    if (query == null && source instanceof DatabaseSource) {
      query = ((DatabaseSource) source).getQuery();
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
    if (source instanceof Resource) {
      return Set.of((Resource) source);
    } else {
      return Set.of();
    }
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rml.LogicalSource);
    if (source != null) {
      if (source instanceof Resource) {
        modelBuilder.add(Rml.source, ((Resource) source).getAsResource());
      } else {
        modelBuilder.add(Rml.source, source);
      }
    }
    if (iterator != null) {
      modelBuilder.add(Rml.iterator, iterator);
    }
    if (referenceFormulation != null) {
      modelBuilder.add(Rml.referenceFormulation, referenceFormulation);
    }
  }
}
