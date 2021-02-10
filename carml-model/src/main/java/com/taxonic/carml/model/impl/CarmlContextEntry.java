package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.CarmlExp;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rml;
import org.eclipse.rdf4j.model.util.ModelBuilder;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class CarmlContextEntry extends CarmlResource implements ContextEntry {

    private String key;
    private String reference;

    public CarmlContextEntry() {
        // Empty constructor for object mapper
    }

    public CarmlContextEntry(
        String key,
        String reference
    ) {
        this.key = key;
        this.reference = reference;
    }

    @RdfProperty(CarmlExp.key)
    @Override
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @RdfProperty(Rml.reference)
    @Override
    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Collections.emptySet();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource())
            .add(Rdf.CarmlExp.key, key)
            .add(Rdf.Rml.reference, reference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, reference);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CarmlContextEntry other = (CarmlContextEntry) obj;
        return Objects.equals(key, other.key) &&
            Objects.equals(reference, other.reference);
    }

    // TODO toString, builder

}
