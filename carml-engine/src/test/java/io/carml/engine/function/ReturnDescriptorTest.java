package io.carml.engine.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

class ReturnDescriptorTest {

    private static final IRI RETURN_IRI = SimpleValueFactory.getInstance().createIRI("http://example.org/return1");

    @Test
    void constructor_throwsIllegalArgument_givenNullType() {
        assertThrows(IllegalArgumentException.class, () -> new ReturnDescriptor(RETURN_IRI, null));
    }

    @Test
    void constructor_allowsNullIri() {
        var descriptor = new ReturnDescriptor(null, String.class);

        assertThat(descriptor, is(notNullValue()));
        assertThat(descriptor.iri(), is(nullValue()));
    }

    @Test
    void constructor_createsInstance_givenValidArguments() {
        var descriptor = new ReturnDescriptor(RETURN_IRI, String.class);

        assertThat(descriptor.iri(), is(RETURN_IRI));
        assertThat(descriptor.type(), is(String.class));
    }
}
