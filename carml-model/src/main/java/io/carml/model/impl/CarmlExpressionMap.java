package io.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import io.carml.model.Condition;
import io.carml.model.ExpressionMap;
import io.carml.model.FunctionExecution;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.Resource;
import io.carml.model.ReturnMap;
import io.carml.model.Template;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.vocab.Carml;
import io.carml.vocab.Fnml;
import io.carml.vocab.OldRml;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import io.carml.vocab.Rr;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;

@SuppressWarnings("java:S1135")
@SuperBuilder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
abstract class CarmlExpressionMap extends CarmlResource implements ExpressionMap {

    private String reference;

    private Template template;

    private Value constant;

    private TriplesMap functionValue;

    private FunctionExecution functionExecution;

    private ReturnMap returnMap;

    @Singular
    private Set<Condition> conditions;

    @RdfProperty(Rml.reference)
    @RdfProperty(OldRml.reference)
    @RdfProperty(Rr.column)
    @RdfProperty(value = Carml.multiReference, deprecated = true)
    @Override
    public String getReference() {
        return reference;
    }

    @RdfProperty(value = Rml.template, handler = TemplatePropertyHandler.class)
    @RdfProperty(value = Rr.template, handler = TemplatePropertyHandler.class)
    @Override
    public Template getTemplate() {
        return template;
    }

    @RdfProperty(Rml.constant)
    @RdfProperty(Rr.constant)
    @Override
    public Value getConstant() {
        return constant;
    }

    @RdfProperty(Fnml.functionValue)
    @RdfProperty(value = Carml.multiFunctionValue, deprecated = true)
    @RdfType(CarmlTriplesMap.class)
    @Override
    public TriplesMap getFunctionValue() {
        return functionValue;
    }

    @RdfProperty(Rml.functionExecution)
    @RdfType(CarmlFunctionExecution.class)
    @Override
    public FunctionExecution getFunctionExecution() {
        return functionExecution;
    }

    @RdfProperty(Rml.returnMap)
    @RdfProperty(Rml.returnProperty)
    @RdfType(CarmlReturnMap.class)
    @Override
    public ReturnMap getReturnMap() {
        return returnMap;
    }

    @RdfProperty(Rml.condition)
    @RdfType(CarmlCondition.class)
    @Override
    public Set<Condition> getConditions() {
        return conditions != null ? conditions : Set.of();
    }

    Set<Resource> getReferencedResourcesBase() {
        var builder = ImmutableSet.<Resource>builder();

        if (functionValue != null) {
            builder.add(functionValue);
        }
        if (functionExecution != null) {
            builder.add(functionExecution);
        }
        if (returnMap != null) {
            builder.add(returnMap);
        }
        if (conditions != null) {
            builder.addAll(conditions);
        }

        return builder.build();
    }

    void addTriplesBase(ModelBuilder builder) {
        if (reference != null) {
            builder.add(Rdf.Rml.reference, reference);
        }
        if (template != null) {
            builder.add(Rdf.Rml.template, template.toTemplateString());
        }
        if (constant != null) {
            builder.add(Rdf.Rml.constant, constant);
        }
        if (functionValue != null) {
            builder.add(Fnml.functionValue, functionValue.getAsResource());
        }
        if (functionExecution != null) {
            builder.add(Rdf.Rml.functionExecution, functionExecution.getAsResource());
        }
        if (returnMap != null) {
            builder.add(Rdf.Rml.returnMap, returnMap.getAsResource());
        }
        if (conditions != null) {
            conditions.forEach(c -> builder.add(Rdf.Rml.condition, c.getAsResource()));
        }
    }

    void adaptReference(UnaryOperator<String> referenceExpressionAdapter, Consumer<String> referenceApplier) {
        var adaptedReference = referenceExpressionAdapter.apply(reference);
        referenceApplier.accept(adaptedReference);
    }

    void adaptTemplate(UnaryOperator<String> referenceExpressionAdapter, Consumer<Template> templateApplier) {
        var prefixedTemplate = template.adaptExpressions(referenceExpressionAdapter);
        templateApplier.accept(prefixedTemplate);
    }

    void adaptFunctionValue(
            UnaryOperator<String> referenceExpressionAdapter, Consumer<TriplesMap> functionValueApplier) {
        var fnBuilder = CarmlTriplesMap.builder();

        functionValue.getSubjectMaps().stream()
                .map(subjectMap -> subjectMap.applyExpressionAdapter(referenceExpressionAdapter))
                .forEach(fnBuilder::subjectMap);

        functionValue.getPredicateObjectMaps().stream()
                .map(pom -> adaptPredicateObjectMap(referenceExpressionAdapter, pom))
                .forEach(fnBuilder::predicateObjectMap);

        functionValueApplier.accept(fnBuilder.build());
    }

    private PredicateObjectMap adaptPredicateObjectMap(
            UnaryOperator<String> referenceExpressionAdapter, PredicateObjectMap predicateObjectMap) {
        var pomBuilder = CarmlPredicateObjectMap.builder();

        predicateObjectMap.getPredicateMaps().stream()
                .map(predicateMap -> predicateMap.applyExpressionAdapter(referenceExpressionAdapter))
                .forEach(pomBuilder::predicateMap);

        // TODO refObjectMap in functionValue?
        predicateObjectMap.getObjectMaps().stream()
                .filter(ObjectMap.class::isInstance)
                .map(ObjectMap.class::cast)
                .map(objectMap -> objectMap.applyExpressionAdapter(referenceExpressionAdapter))
                .forEach(pomBuilder::objectMap);

        return pomBuilder.build();
    }
}
