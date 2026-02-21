package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.ExpressionField;
import io.carml.model.Join;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.Resource;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class CarmlLogicalViewJoin extends CarmlResource implements LogicalViewJoin {

    private LogicalView parentLogicalView;

    @Singular
    private Set<Join> joinConditions;

    @Singular
    private Set<ExpressionField> fields;

    @RdfProperty(Rml.parentLogicalView)
    @RdfType(CarmlLogicalView.class)
    @Override
    public LogicalView getParentLogicalView() {
        return parentLogicalView;
    }

    @RdfProperty(Rml.joinCondition)
    @RdfType(CarmlJoin.class)
    @Override
    public Set<Join> getJoinConditions() {
        return joinConditions;
    }

    @RdfProperty(Rml.field)
    @RdfType(CarmlExpressionField.class)
    @Override
    public Set<ExpressionField> getFields() {
        return fields;
    }

    @Override
    public Set<Resource> getReferencedResources() {
        var builder = ImmutableSet.<Resource>builder();

        if (parentLogicalView != null) {
            builder.add(parentLogicalView);
        }
        if (joinConditions != null) {
            builder.addAll(joinConditions);
        }
        if (fields != null) {
            builder.addAll(fields);
        }

        return builder.build();
    }

    @Override
    public void addTriples(ModelBuilder modelBuilder) {
        modelBuilder.subject(getAsResource()).add(RDF.TYPE, Rdf.Rml.LogicalViewJoin);

        if (parentLogicalView != null) {
            modelBuilder.add(Rdf.Rml.parentLogicalView, parentLogicalView.getAsResource());
        }
        if (joinConditions != null) {
            joinConditions.forEach(jc -> modelBuilder.add(Rdf.Rml.joinCondition, jc.getAsResource()));
        }
        if (fields != null) {
            fields.forEach(f -> modelBuilder.add(Rdf.Rml.field, f.getAsResource()));
        }
    }
}
