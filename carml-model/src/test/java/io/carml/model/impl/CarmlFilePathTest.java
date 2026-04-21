package io.carml.model.impl;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.carml.model.FilePath;
import io.carml.model.Source;
import io.carml.model.Target;
import org.junit.jupiter.api.Test;

/**
 * Pins the dual-use behavior of {@code rml:FilePath}: per RML-IO spec it addresses both input
 * {@link Source}s and output {@link Target}s, so the concrete {@link CarmlFilePath} must satisfy
 * both interface contracts with the encoding/compression getters unified under the shared
 * signature.
 */
class CarmlFilePathTest {

    @Test
    void carmlFilePath_instanceOf_bothSourceAndTarget() {
        // Given
        var filePath = CarmlFilePath.builder().path("Friends.json").build();

        // Then — the dual-use bridge is how RML-IO mappings with `rml:target [ a rml:FilePath ]`
        // resolve cleanly to the Target-typed property on LogicalTarget.
        assertThat(filePath, instanceOf(Source.class));
        assertThat(filePath, instanceOf(Target.class));
        assertThat(filePath, instanceOf(FilePath.class));
    }

    @Test
    void getSerialization_noFieldOnFilePath_returnsNull() {
        // Given — serialization lives on LogicalTarget in RML-IO; FilePath has no dedicated field.
        var filePath = CarmlFilePath.builder().path("out.nq").build();

        // Then — always null; callers must consult the enclosing LogicalTarget for an effective
        // serialization.
        assertThat(filePath.getSerialization(), is(nullValue()));
    }

    @Test
    void getEncoding_setViaSource_returnsSameValue() {
        // Given — encoding IRI is shared between the Source and Target contracts; CarmlFilePath
        // inherits a single implementation from CarmlSource that satisfies both.
        var encoding = iri("http://w3id.org/rml/UTF-16");
        var filePath = CarmlFilePath.builder().path("out.nq").encoding(encoding).build();

        // Then — the same getter satisfies both interfaces
        assertThat(filePath.getEncoding(), is(encoding));
        assertThat(filePath.getEncoding(), is(encoding));
        assertThat(((Target) filePath).getEncoding(), is(encoding));
    }

    @Test
    void getCompression_setViaSource_returnsSameValue() {
        // Given — same rationale as encoding: unified under Source/Target common signature.
        var compression = iri("http://w3id.org/rml/gzip");
        var filePath =
                CarmlFilePath.builder().path("out.nq").compression(compression).build();

        // Then
        assertThat(filePath.getCompression(), is(compression));
        assertThat(filePath.getCompression(), is(compression));
        assertThat(((Target) filePath).getCompression(), is(compression));
    }

    @Test
    void getPath_returnsBuilderPath() {
        // Given
        var filePath = CarmlFilePath.of("Friends.json");

        // Then
        assertThat(filePath, is(notNullValue()));
        assertThat(filePath.getPath(), is("Friends.json"));
    }
}
