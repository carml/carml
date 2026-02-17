package io.carml.testcases.model;

import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.model.Resource;
import io.carml.model.impl.CarmlResource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuppressWarnings("java:S2187")
@Setter
@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TestCase extends CarmlResource {

    private String identifier;

    private boolean hasError;

    @Singular
    private Set<Input> inputs;

    @Singular
    private Set<String> outputs;

    private String mappingDocument;

    @RdfProperty("http://purl.org/dc/terms/identifier")
    public String getIdentifier() {
        return identifier;
    }

    @RdfProperty("http://w3id.org/rml/test/hasError")
    public boolean hasError() {
        return hasError;
    }

    @RdfProperty("http://w3id.org/rml/test/input")
    @RdfType(Input.class)
    public Set<Input> getInputs() {
        return inputs;
    }

    @RdfProperty(value = "http://w3id.org/rml/test/output", handler = IriFilenamePropertyHandler.class)
    public Set<String> getOutputs() {
        return outputs;
    }

    @RdfProperty("http://w3id.org/rml/test/mappingDocument")
    public String getMappingDocument() {
        return mappingDocument;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        if (inputs != null && !inputs.isEmpty()) {
            return Set.copyOf(inputs);
        }

        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, iri("http://www.w3.org/2006/03/test-description#TestCase"));

        if (identifier != null) {
            modelBuilder.add(DCTERMS.IDENTIFIER, identifier);
        }

        modelBuilder.add(iri("http://w3id.org/rml/test/hasError"), hasError);

        if (inputs != null) {
            inputs.forEach(input -> modelBuilder.add(iri("http://w3id.org/rml/test/input"), input.getAsResource()));
        }

        if (outputs != null) {
            outputs.forEach(output -> modelBuilder.add(iri("http://w3id.org/rml/test/output"), output));
        }

        if (mappingDocument != null) {
            modelBuilder.add(iri("http://w3id.org/rml/test/mappingDocument"), mappingDocument);
        }
    }
}
