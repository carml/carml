package io.carml.engine.rdf;

import com.google.common.collect.Sets;
import io.carml.engine.MappedValue;
import io.carml.engine.rdf.RdfContainer.RdfContainerBuilder;
import io.carml.engine.rdf.RdfList.RdfListBuilder;
import io.carml.engine.rdf.util.RdfCollectionsAndContainers;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.GatherMap;
import io.carml.vocab.Rdf.Rml;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@AllArgsConstructor(staticName = "of")
public class RdfListOrContainerGenerator
        implements BiFunction<ExpressionEvaluation, DatatypeMapper, List<MappedValue<Value>>> {

    private GatherMap gatherMap;

    private ValueFactory valueFactory;

    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @Override
    public List<MappedValue<Value>> apply(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        var headGenerator = rdfTermGeneratorFactory.getSubjectGenerator(gatherMap.asSubjectMap());
        var strategy = gatherMap.getStrategy();

        if (strategy.equals(Rml.append)) {
            var appendedGatheredTerms = gatherMap.getGathers().stream()
                    .map(termMap -> rdfTermGeneratorFactory.getObjectGenerator(termMap))
                    .map(termGenerator -> termGenerator.apply(expressionEvaluation, datatypeMapper))
                    .flatMap(List::stream)
                    .toList();

            if (appendedGatheredTerms.isEmpty()) {
                return handleEmpty();
            }

            return headGenerator.apply(expressionEvaluation, datatypeMapper).stream()
                    .map(head -> createRdfListOrContainer(head, appendedGatheredTerms))
                    .filter(Objects::nonNull)
                    .toList();
        }

        var gatheredTerms = gatherMap.getGathers().stream()
                .map(termMap -> rdfTermGeneratorFactory.getObjectGenerator(termMap))
                .map(termGenerator -> termGenerator.apply(expressionEvaluation, datatypeMapper))
                .map(LinkedHashSet::new) // TODO https://github.com/kg-construct/rml-core/issues/121
                .toList();

        if (gatheredTerms.isEmpty()) {
            return handleEmpty();
        }

        return Sets.cartesianProduct(gatheredTerms).stream()
                .flatMap(cartesianProductItem -> headGenerator.apply(expressionEvaluation, datatypeMapper).stream()
                        .map(head -> createRdfListOrContainer(head, cartesianProductItem)))
                .toList();
    }

    private List<MappedValue<Value>> handleEmpty() {
        if (gatherMap.getAllowEmptyListAndContainer()) {
            return List.of(RdfMappedValue.of(RDF.NIL, gatherMap.getTargets()));
        } else {
            return List.of();
        }
    }

    private MappedValue<Value> createRdfListOrContainer(
            MappedValue<Resource> headOrContainer, Collection<MappedValue<Value>> gatheredTerms) {
        var mergeable = !gatherMap.getExpressionMapExpressionSet().isEmpty();
        if (gatherMap.getGatherAs().equals(RDF.LIST)) {
            return createRdfList(mergeable, headOrContainer, gatheredTerms);
        }
        return createRdfContainer(mergeable, headOrContainer, gatheredTerms);
    }

    private MappedValue<Value> createRdfList(
            boolean mergeable, MappedValue<Resource> head, Collection<MappedValue<Value>> gatheredTerms) {
        if (mergeable) {
            return createRdfList(head, gatheredTerms, MergeableRdfList::builder);
        }

        return createRdfList(head, gatheredTerms, RdfList::builder);
    }

    private RdfList<Value> createRdfList(
            MappedValue<Resource> head,
            Collection<MappedValue<Value>> gatheredTerms,
            Supplier<RdfListBuilder<Value, ?, ?>> builderSupplier) {
        var headResource = head.getValue();

        var gatheredResources = gatheredTerms.stream() //
                .map(MappedValue::getValue)
                .toList();

        var mappedValueModel = getNestedMappedValueModel(gatheredTerms);

        var model = RdfCollectionsAndContainers.toRdfListModel(
                gatheredResources, headResource, mappedValueModel, valueFactory);

        return builderSupplier.get().head(head.getValue()).model(model).build();
    }

    private MappedValue<Value> createRdfContainer(
            boolean mergeable, MappedValue<Resource> container, Collection<MappedValue<Value>> gatheredTerms) {
        if (mergeable) {
            return createRdfContainer(container, gatheredTerms, MergeableRdfContainer::builder);
        }

        return createRdfContainer(container, gatheredTerms, RdfContainer::builder);
    }

    private RdfContainer<Value> createRdfContainer(
            MappedValue<Resource> container,
            Collection<MappedValue<Value>> gatheredTerms,
            Supplier<RdfContainerBuilder<Value, ?, ?>> builderSupplier) {
        var containerResource = container.getValue();

        var gatheredResources = gatheredTerms.stream() //
                .map(MappedValue::getValue)
                .toList();

        var mappedValueModel = getNestedMappedValueModel(gatheredTerms);

        var model = RdfCollectionsAndContainers.toRdfContainerModel(
                gatherMap.getGatherAs(), gatheredResources, containerResource, mappedValueModel, valueFactory);

        return builderSupplier
                .get()
                .type(gatherMap.getGatherAs())
                .container(containerResource)
                .model(model)
                .build();
    }

    private Model getNestedMappedValueModel(Collection<MappedValue<Value>> gatheredTerms) {
        return gatheredTerms.stream()
                .filter(ModelBearing.class::isInstance)
                .map(ModelBearing.class::cast)
                .map(ModelBearing::getModel)
                .flatMap(Model::stream)
                .collect(ModelCollector.toModel());
    }
}
