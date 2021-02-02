package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.CarmlExp;
import com.taxonic.carml.vocab.Rdf;
import org.eclipse.rdf4j.model.util.ModelBuilder;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class CarmlContextEntry extends CarmlResource implements ContextEntry {

    private String key;
    private String valueReference;

    public CarmlContextEntry() {
        // Empty constructor for object mapper
    }

    public CarmlContextEntry(
        String key,
        String valueReference
    ) {
        this.key = key;
        this.valueReference = valueReference;
    }

    @RdfProperty(CarmlExp.key)
    @Override
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @RdfProperty(CarmlExp.valueReference)
    @Override
    public String getValueReference() {
        return valueReference;
    }

    public void setValueReference(String valueReference) {
        this.valueReference = valueReference;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        return Collections.emptySet();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource())
            .add(Rdf.CarmlExp.key, key)
            .add(Rdf.CarmlExp.valueReference, valueReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, valueReference);
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
            Objects.equals(valueReference, other.valueReference);
    }

    // TODO toString, builder

}
