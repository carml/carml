package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalTable;
import io.carml.model.PredicateObjectMap;
import io.carml.model.Resource;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.OldRml;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import java.util.StringJoiner;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@EqualsAndHashCode
public class CarmlTriplesMap extends CarmlResource implements TriplesMap {

  private LogicalSource logicalSource;

  private LogicalTable logicalTable;

  @Singular
  private Set<SubjectMap> subjectMaps;

  @Singular
  private Set<PredicateObjectMap> predicateObjectMaps;

  @RdfProperty(Rml.logicalSource)
  @RdfProperty(OldRml.logicalSource)
  @RdfType(CarmlLogicalSource.class)
  @Override
  public LogicalSource getLogicalSource() {
    return logicalTable != null ? logicalTable : logicalSource;
  }

  @RdfProperty(Rr.logicalTable)
  @RdfType(CarmlLogicalTable.class)
  @Override
  public LogicalTable getLogicalTable() {
    return logicalTable;
  }

  @RdfProperty(Rml.subjectMap)
  @RdfProperty(Rr.subjectMap)
  @RdfType(CarmlSubjectMap.class)
  @Override
  public Set<SubjectMap> getSubjectMaps() {
    return subjectMaps;
  }

  @RdfProperty(Rml.predicateObjectMap)
  @RdfProperty(Rr.predicateObjectMap)
  @RdfType(CarmlPredicateObjectMap.class)
  @Override
  public Set<PredicateObjectMap> getPredicateObjectMaps() {
    return predicateObjectMaps;
  }

  @Override
  public String toString() {
    return new StringJoiner(String.format(",%s", System.lineSeparator()), "CarmlTriplesMap(", ")")
        .add(String.format("super=%s", super.toString()))
        .add(String.format("logicalSource=%s", logicalSource))
        .add(String.format("logicalTable=%s", logicalTable))
        .add(String.format("subjectMaps=%s", subjectMaps))
        .add(String.format("predicateObjectMaps=%s", predicateObjectMaps))
        .toString();
  }

  @Override
  public Set<Resource> getReferencedResources() {
    var builder = ImmutableSet.<Resource>builder();
    if (logicalSource != null) {
      builder.add(logicalSource);
    }
    if (logicalTable != null) {
      builder.add(logicalTable);
    }
    return builder.addAll(subjectMaps)
        .addAll(predicateObjectMaps)
        .build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rml.TriplesMap);
    if (logicalSource != null) {
      modelBuilder.add(Rdf.Rml.logicalSource, logicalSource.getAsResource());
    }
    if (logicalTable != null) {
      modelBuilder.add(Rdf.Rr.logicalTable, logicalTable.getAsResource());
    }
    subjectMaps.forEach(sm -> modelBuilder.add(Rdf.Rml.subjectMap, sm.getAsResource()));
    predicateObjectMaps.forEach(pom -> modelBuilder.add(Rdf.Rml.predicateObjectMap, pom.getAsResource()));
  }
}
