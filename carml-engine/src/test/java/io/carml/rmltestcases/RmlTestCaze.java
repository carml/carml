package io.carml.rmltestcases;

import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.rmltestcases.model.Dataset;
import io.carml.rmltestcases.model.Input;
import io.carml.rmltestcases.model.Output;
import io.carml.rmltestcases.model.Rules;
import io.carml.rmltestcases.model.TestCase;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;

public class RmlTestCaze extends RtcResource implements TestCase {

  private String identifier;

  private String description;

  private IRI specificationReference;

  private Set<Dataset> parts;

  private Rules rules;

  private Output output;

  @RdfProperty("http://purl.org/dc/terms/identifier")
  @Override
  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  @RdfProperty("http://purl.org/dc/terms/description")
  @Override
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @RdfProperty("http://www.w3.org/2006/03/test-description#specificationReference")
  @Override
  public IRI getSpecificationReference() {
    return specificationReference;
  }

  public void setSpecificationReference(IRI specificationReference) {
    this.specificationReference = specificationReference;
  }

  @RdfProperty("http://purl.org/dc/terms/hasPart")
  @RdfType(RtcDataset.class)
  @Override
  public Set<Dataset> getParts() {
    return parts;
  }

  public void setParts(Set<Dataset> parts) {
    this.parts = parts;
  }

  @RdfProperty("http://rml.io/ns/test-case/rules")
  @RdfType(RtcRules.class)
  @Override
  public Rules getRules() {
    return rules;
  }

  public void setRules(Rules rules) {
    this.rules = rules;
  }

  @Override
  public Set<Input> getInput() {
    return parts.stream() //
        .filter(p -> p.getId()
            .contains("/input")) //
        .map(Input.class::cast) //
        .collect(Collectors.toUnmodifiableSet());
  }

  @RdfProperty("http://www.w3.org/2006/03/test-description#expectedResults")
  @RdfType(RtcOutput.class)
  @Override
  public Output getOutput() {
    return output;
  }

  public void setOutput(Output output) {
    this.output = output;
  }

  @Override
  public String toString() {
    return String.format("%s: %s", identifier, description);
  }

}
