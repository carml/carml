package io.carml.rmltestcases.model;

import java.util.Set;
import org.eclipse.rdf4j.model.IRI;

public interface TestCase extends Resource {

    String getIdentifier();

    String getDescription();

    IRI getSpecificationReference();

    Set<Dataset> getParts();

    Rules getRules();

    Set<Input> getInput();

    Output getOutput();
}
