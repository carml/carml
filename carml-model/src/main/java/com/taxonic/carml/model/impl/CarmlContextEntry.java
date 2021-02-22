package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.CarmlExp;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rml;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class CarmlContextEntry extends CarmlResource implements ContextEntry {

    private String as;
    private String reference;

    public CarmlContextEntry() {
        // Empty constructor for object mapper
    }

    public CarmlContextEntry(
        String as,
        String reference
    ) {
        this.as = as;
        this.reference = reference;
    }

    @RdfProperty(CarmlExp.as)
    @Override
    public String getAs() {
        return as;
    }

    public void setAs(String as) {
        this.as = as;
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
            .add(Rdf.CarmlExp.as, as)
            .add(Rdf.Rml.reference, reference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(as, reference);
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
        return Objects.equals(as, other.as) &&
            Objects.equals(reference, other.reference);
    }

    @Override
    public String toString() {
        return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
    }
}
