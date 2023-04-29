package io.carml.model.impl;

import io.carml.model.Join;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rr;
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
public class CarmlJoin extends CarmlResource implements Join {

  private String child;

  private String parent;

  @RdfProperty(Rr.child)
  @Override
  public String getChild() {
    return child;
  }

  @RdfProperty(Rr.parent)
  @Override
  public String getParent() {
    return parent;
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return Set.of();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Rr.Join);
    if (child != null) {
      modelBuilder.add(Rr.child, child);
    }
    if (parent != null) {
      modelBuilder.add(Rr.parent, parent);
    }
  }
}
