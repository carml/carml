package io.carml.model.impl;

import io.carml.model.Namespace;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.vocab.Carml;
import io.carml.vocab.Rdf;
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
public class CarmlNamespace extends CarmlResource implements Namespace {

  private String prefix;

  private String name;

  @RdfProperty(Carml.namespacePrefix)
  @Override
  public String getPrefix() {
    return prefix;
  }

  @RdfProperty(Carml.namespaceName)
  @Override
  public String getName() {
    return name;
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return Set.of();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource())
        .add(RDF.TYPE, Rdf.Carml.Namespace);
    if (prefix != null) {
      modelBuilder.add(Carml.namespacePrefix, prefix);
    }
    if (name != null) {
      modelBuilder.add(Carml.namespaceName, name);
    }
  }

}
