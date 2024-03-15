package io.carml.testcases.model;

import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.model.Resource;
import io.carml.model.impl.CarmlResource;
import io.carml.rdfmapper.annotations.RdfProperty;
import java.util.List;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@Setter
@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TripleStore extends CarmlResource {

  @Singular
  private List<String> rdfFiles;

  @RdfProperty("http://w3id.org/rml/test/rdfFile")
  public List<String> getRdfFiles() {
    return rdfFiles;
  }

  @Override
  public Set<Resource> getReferencedResources() {
    return Set.of();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource());

    rdfFiles.forEach(rdfFile -> modelBuilder.add(iri("http://w3id.org/rml/test/rdfFile"), rdfFile));
  }
}
