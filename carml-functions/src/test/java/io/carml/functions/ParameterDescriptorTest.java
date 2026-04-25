package io.carml.functions;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.eclipse.rdf4j.model.IRI;
import org.junit.jupiter.api.Test;

class ParameterDescriptorTest {

    private static final IRI PARAM_IRI = iri("http://example.org/param1");

    @Test
    void constructor_throwsIllegalArgument_givenNullIri() {
        assertThrows(IllegalArgumentException.class, () -> new ParameterDescriptor(null, String.class, true));
    }

    @Test
    void constructor_throwsIllegalArgument_givenNullType() {
        assertThrows(IllegalArgumentException.class, () -> new ParameterDescriptor(PARAM_IRI, null, true));
    }

    @Test
    void constructor_createsInstance_givenValidArguments() {
        var descriptor = new ParameterDescriptor(PARAM_IRI, String.class, true);

        assertThat(descriptor, is(notNullValue()));
    }

    @Test
    void accessors_returnCorrectValues() {
        var descriptor = new ParameterDescriptor(PARAM_IRI, Integer.class, false);

        assertThat(descriptor.parameterIri(), is(PARAM_IRI));
        assertThat(descriptor.type(), is(Integer.class));
        assertThat(descriptor.required(), is(false));
    }

    @Test
    void matches_returnsTrue_givenPredicateIri() {
        var descriptor = new ParameterDescriptor(PARAM_IRI, String.class, true);

        assertThat(descriptor.matches(PARAM_IRI), is(true));
    }

    @Test
    void matches_returnsFalse_givenUnrelatedIri() {
        var descriptor = new ParameterDescriptor(PARAM_IRI, String.class, true);

        assertThat(descriptor.matches(iri("http://example.org/other")), is(false));
    }
}
