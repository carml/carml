package io.carml.model.impl;

import io.carml.model.Source;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.IRI;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
abstract class CarmlSource extends CarmlResource implements Source {

    private IRI encoding;

    private String nullPattern;

    private IRI compression;

    @Override
    public IRI getEncoding() {
        return encoding;
    }

    @Override
    public String getNull() {
        return nullPattern;
    }

    @Override
    public IRI getCompression() {
        return compression;
    }
}
