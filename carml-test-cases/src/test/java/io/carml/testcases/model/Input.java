package io.carml.testcases.model;

import com.google.common.collect.ImmutableSet;
import io.carml.model.Resource;
import io.carml.model.impl.CarmlResource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
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
public class Input extends CarmlResource {

  private String inputType;

  @Singular
  private List<String> inputFiles;

  private Database database;

  private TripleStore tripleStore;

  @RdfProperty("http://w3id.org/rml/test/inputType")
  public String getInputType() {
    return inputType;
  }

  @RdfProperty("http://w3id.org/rml/test/inputFile")
  public List<String> getInputFiles() {
    return inputFiles;
  }

  @RdfProperty("http://w3id.org/rml/test/database")
  @RdfType(Database.class)
  public Database getDatabase() {
    return database;
  }

  @RdfProperty("http://w3id.org/rml/test/tripleStore")
  @RdfType(TripleStore.class)
  public TripleStore getTripleStore() {
    return tripleStore;
  }

  @Override
  public Set<Resource> getReferencedResources() {
    var builder = ImmutableSet.<Resource>builder();

    if (database != null) {
      builder.add(database);
    }

    if (tripleStore != null) {
      builder.add(tripleStore);
    }

    return builder.build();
  }

  @Override
  public void addTriples(ModelBuilder modelBuilder) {
    modelBuilder.subject(getAsResource());

    if (inputType != null) {
      modelBuilder.add("http://w3id.org/rml/test/inputType", inputType);
    }

    inputFiles.forEach(inputFile -> modelBuilder.add("http://w3id.org/rml/test/inputFile", inputFile));

    if (database != null) {
      modelBuilder.add("http://w3id.org/rml/test/database", database.getAsResource());
    }

    if (tripleStore != null) {
      modelBuilder.add("http://w3id.org/rml/test/tripleStore", tripleStore.getAsResource());
    }
  }
}
