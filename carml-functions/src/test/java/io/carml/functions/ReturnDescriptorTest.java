package io.carml.functions;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.eclipse.rdf4j.model.IRI;
import org.junit.jupiter.api.Test;

class ReturnDescriptorTest {

    private static final IRI OUTPUT_IRI = iri("http://example.org/output1");

    @Test
    void constructor_throwsIllegalArgument_givenNullType() {
        assertThrows(IllegalArgumentException.class, () -> new ReturnDescriptor(OUTPUT_IRI, null));
    }

    @Test
    void constructor_allowsNullIri() {
        var descriptor = new ReturnDescriptor(null, String.class);

        assertThat(descriptor, is(notNullValue()));
        assertThat(descriptor.outputIri(), is(nullValue()));
    }

    @Test
    void constructor_createsInstance_givenValidArguments() {
        var descriptor = new ReturnDescriptor(OUTPUT_IRI, String.class);

        assertThat(descriptor.outputIri(), is(OUTPUT_IRI));
        assertThat(descriptor.type(), is(String.class));
    }

    @Test
    void matches_returnsTrue_givenOutputIri() {
        var descriptor = new ReturnDescriptor(OUTPUT_IRI, String.class);

        assertThat(descriptor.matches(OUTPUT_IRI), is(true));
    }

    @Test
    void matches_returnsFalse_givenUnrelatedIri() {
        var descriptor = new ReturnDescriptor(OUTPUT_IRI, String.class);

        assertThat(descriptor.matches(iri("http://example.org/other")), is(false));
    }

    @Test
    void matches_returnsFalse_whenOutputIriIsNull() {
        var descriptor = new ReturnDescriptor(null, String.class);

        assertThat(descriptor.matches(iri("http://example.org/other")), is(false));
    }
}
