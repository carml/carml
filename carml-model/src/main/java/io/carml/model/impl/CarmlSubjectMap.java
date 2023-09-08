package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.GraphMap;
import io.carml.model.Resource;
import io.carml.model.SubjectMap;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rr;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CarmlSubjectMap extends CarmlTermMap implements SubjectMap {

  @Singular("clazz")
  private Set<IRI> classes;

  @Singular
  private Set<GraphMap> graphMaps;

  @RdfProperty(Rr.graphMap)
  @RdfType(CarmlGraphMap.class)
  @Override
  public Set<GraphMap> getGraphMaps() {
    return graphMaps;
  }

  public void setGraphMaps(Set<GraphMap> graphMaps) {
    this.graphMaps = graphMaps;
  }

  @RdfProperty(Rr.clazz)
  @Override
  public Set<IRI> getClasses() {
    return classes;
  }

  public void setClasses(Set<IRI> classes) {
    this.classes = classes;
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return ImmutableSet.<Resource>builder()
        .addAll(getReferencedResourcesBase())
        .addAll(graphMaps)
        .build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rr.SubjectMap);

    addTriplesBase(modelBuilder);

    graphMaps.forEach(gm -> modelBuilder.add(Rr.graphMap, gm.getAsResource()));
    classes.forEach(cl -> modelBuilder.add(Rr.clazz, cl));
  }
}
