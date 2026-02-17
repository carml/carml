package io.carml.testcases.model;

import io.carml.model.Resource;
import io.carml.model.impl.CarmlResource;
import io.carml.rdfmapper.annotations.RdfProperty;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@Setter
@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class Output extends CarmlResource {

    private String output;

    private String outputFormat;

    @RdfProperty("http://w3id.org/rml/test/output")
    public String getOutput() {
        return output;
    }

    @RdfProperty("http://w3id.org/rml/test/outputFormat")
    public String getOutputFormat() {
        return outputFormat;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Set.of();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource());

        if (output != null) {
            modelBuilder.add("http://w3id.org/rml/test/output", output);
        }

        if (outputFormat != null) {
            modelBuilder.add("http://w3id.org/rml/test/outputFormat", outputFormat);
        }
    }
}
