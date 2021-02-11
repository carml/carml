package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.NestedMapping;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.CarmlExp;
import com.taxonic.carml.vocab.Rdf;
import org.apache.commons.lang3.ObjectUtils;
import org.eclipse.rdf4j.model.util.ModelBuilder;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class CarmlNestedMapping extends CarmlResource implements NestedMapping {

    private TriplesMap triplesMap;
    private Set<ContextEntry> contextEntries;

    public CarmlNestedMapping() {
        // Empty constructor for object mapper
    }

    public CarmlNestedMapping(
        TriplesMap triplesMap,
        Set<ContextEntry> contextEntries
    ) {
        this.triplesMap = triplesMap;
        this.contextEntries = contextEntries;
    }

    @RdfProperty(CarmlExp.subTriplesMap)
    @RdfType(CarmlTriplesMap.class)
    @Override
    public TriplesMap getTriplesMap() {
        return triplesMap;
    }

    public void setTriplesMap(TriplesMap triplesMap) {
        this.triplesMap = triplesMap;
    }

    @RdfProperty(CarmlExp.context)
    @RdfType(CarmlContextEntry.class)
    @Override
    public Set<ContextEntry> getContextEntries() {
        return contextEntries;
    }

    public void setContextEntries(Set<ContextEntry> contextEntries) {
        this.contextEntries = contextEntries;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        ImmutableSet.Builder<Resource> builder = ImmutableSet.builder();
        if (triplesMap != null) {
            builder.add(triplesMap);
        }
        return builder
            .addAll(ObjectUtils.defaultIfNull(contextEntries, Collections.<ContextEntry>emptySet()))
            .build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource());
        if (triplesMap != null) {
            modelBuilder.add(Rdf.CarmlExp.subTriplesMap, triplesMap.getAsResource());
        }
        ObjectUtils.defaultIfNull(contextEntries, Collections.<ContextEntry>emptySet())
            .forEach(e -> modelBuilder.add(Rdf.CarmlExp.context, e.getAsResource()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(triplesMap, contextEntries);
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
        CarmlNestedMapping other = (CarmlNestedMapping) obj;
        return Objects.equals(triplesMap, other.triplesMap) &&
            Objects.equals(contextEntries, other.contextEntries);
    }

    // TODO toString, builder

}
