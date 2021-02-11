package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.MergeSuper;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.CarmlExp;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rml;
import org.apache.commons.lang3.ObjectUtils;
import org.eclipse.rdf4j.model.util.ModelBuilder;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class CarmlMergeSuper extends CarmlResource implements MergeSuper {

    private LogicalSource logicalSource;
    private Set<ContextEntry> including;

    public CarmlMergeSuper() {
        // Empty constructor for object mapper
    }

    public CarmlMergeSuper(
        LogicalSource logicalSource,
        Set<ContextEntry> including
    ) {
        this.logicalSource = logicalSource;
        this.including = including;
    }

    @RdfProperty(CarmlExp.fromLogicalSource)
    @RdfType(CarmlLogicalSource.class)
    @Override
    public LogicalSource getLogicalSource() {
        return logicalSource;
    }

    public void setLogicalSource(LogicalSource logicalSource) {
        this.logicalSource = logicalSource;
    }

    @RdfProperty(CarmlExp.including)
    @RdfType(CarmlContextEntry.class)
    @Override
    public Set<ContextEntry> getIncluding() {
        return including;
    }

    public void setIncluding(Set<ContextEntry> including) {
        this.including = including;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        ImmutableSet.Builder<Resource> builder = ImmutableSet.builder();
        if (logicalSource != null) {
            builder.add(logicalSource);
        }
        return builder
            .addAll(ObjectUtils.defaultIfNull(including, Collections.<ContextEntry>emptySet()))
            .build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource());
        if (logicalSource != null) {
            modelBuilder.add(Rdf.CarmlExp.fromLogicalSource, logicalSource.getAsResource());
        }
        ObjectUtils.defaultIfNull(including, Collections.<ContextEntry>emptySet())
            .forEach(e -> modelBuilder.add(Rdf.CarmlExp.including, e.getAsResource()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalSource, including);
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
        CarmlMergeSuper other = (CarmlMergeSuper) obj;
        return Objects.equals(logicalSource, other.logicalSource) &&
            Objects.equals(including, other.including);
    }

    // TODO toString, builder

}
