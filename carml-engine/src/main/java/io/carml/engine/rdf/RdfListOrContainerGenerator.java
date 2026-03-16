package io.carml.engine.rdf;

import static io.carml.util.CartesianProduct.listCartesianProduct;

import io.carml.engine.MappedValue;
import io.carml.engine.TermGenerator;
import io.carml.engine.TermGeneratorFactoryException;
import io.carml.engine.rdf.RdfContainer.RdfContainerBuilder;
import io.carml.engine.rdf.RdfList.RdfListBuilder;
import io.carml.engine.rdf.util.RdfCollectionsAndContainers;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.BaseObjectMap;
import io.carml.model.GatherMap;
import io.carml.model.ObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.vocab.Rdf.Rml;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class RdfListOrContainerGenerator
        implements BiFunction<ExpressionEvaluation, DatatypeMapper, List<MappedValue<Value>>> {

    private final GatherMap gatherMap;

    private final ValueFactory valueFactory;

    private final RdfTermGeneratorFactory rdfTermGeneratorFactory;

    private final Map<RefObjectMap, String> refObjectMapPrefixes;

    private RdfListOrContainerGenerator(
            GatherMap gatherMap,
            ValueFactory valueFactory,
            RdfTermGeneratorFactory rdfTermGeneratorFactory,
            Map<RefObjectMap, String> refObjectMapPrefixes) {
        this.gatherMap = gatherMap;
        this.valueFactory = valueFactory;
        this.rdfTermGeneratorFactory = rdfTermGeneratorFactory;
        this.refObjectMapPrefixes = refObjectMapPrefixes;
    }

    public static RdfListOrContainerGenerator of(
            GatherMap gatherMap, ValueFactory valueFactory, RdfTermGeneratorFactory rdfTermGeneratorFactory) {
        return new RdfListOrContainerGenerator(gatherMap, valueFactory, rdfTermGeneratorFactory, Map.of());
    }

    public static RdfListOrContainerGenerator of(
            GatherMap gatherMap,
            ValueFactory valueFactory,
            RdfTermGeneratorFactory rdfTermGeneratorFactory,
            Map<RefObjectMap, String> refObjectMapPrefixes) {
        return new RdfListOrContainerGenerator(gatherMap, valueFactory, rdfTermGeneratorFactory, refObjectMapPrefixes);
    }

    @Override
    public List<MappedValue<Value>> apply(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        var headGenerator = rdfTermGeneratorFactory.getSubjectGenerator(gatherMap.asSubjectMap());
        var strategy = gatherMap.getStrategy();

        var gatherTermGenerators = gatherMap.getGathers().stream()
                .map(this::createGatherTermGenerator)
                .toList();

        if (strategy.equals(Rml.append)) {
            var appendedGatheredTerms = gatherTermGenerators.stream()
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

        var gatheredTerms = gatherTermGenerators.stream()
                .map(termGenerator -> termGenerator.apply(expressionEvaluation, datatypeMapper))
                .toList();

        if (gatheredTerms.isEmpty()) {
            return handleEmpty();
        }

        return listCartesianProduct(gatheredTerms).stream()
                .flatMap(cartesianProductItem -> headGenerator.apply(expressionEvaluation, datatypeMapper).stream()
                        .map(head -> createRdfListOrContainer(head, cartesianProductItem)))
                .toList();
    }

    @SuppressWarnings("unchecked")
    private TermGenerator<Value> createGatherTermGenerator(BaseObjectMap gatherItem) {
        if (gatherItem instanceof ObjectMap objectMap) {
            return rdfTermGeneratorFactory.getObjectGenerator(objectMap);
        }

        if (gatherItem instanceof RefObjectMap refObjectMap) {
            if (!refObjectMap.getJoinConditions().isEmpty()) {
                return createJoinedRefObjectMapGatherGenerator(refObjectMap);
            }

            // Joinless RefObjectMap: generate parent subjects from the current row's expression
            // evaluation. This works because the parent TriplesMap shares the same logical source.
            var parentSubjectGenerators = refObjectMap.getParentTriplesMap().getSubjectMaps().stream()
                    .map(rdfTermGeneratorFactory::getSubjectGenerator)
                    .toList();

            return (expressionEvaluation, datatypeMapper) -> parentSubjectGenerators.stream()
                    .map(gen -> gen.apply(expressionEvaluation, datatypeMapper))
                    .flatMap(List::stream)
                    .map(mappedValue -> (MappedValue<Value>) (MappedValue<?>) mappedValue)
                    .toList();
        }

        throw new TermGeneratorFactoryException("Unsupported gather item type: %s"
                .formatted(gatherItem.getClass().getSimpleName()));
    }

    /**
     * Creates a term generator for a joined RefObjectMap within a gather map. The aggregating join
     * provides list-valued fields (all matching parent values for each child row). This generator
     * iterates over the list positions and generates one parent subject per position.
     */
    @SuppressWarnings("unchecked")
    private TermGenerator<Value> createJoinedRefObjectMapGatherGenerator(RefObjectMap refObjectMap) {
        var prefix = refObjectMapPrefixes.get(refObjectMap);
        if (prefix == null) {
            throw new TermGeneratorFactoryException("No prefix found for joined RefObjectMap in gather. "
                    + "Ensure ImplicitViewFactory processes gather joining RefObjectMaps.");
        }

        // Adapt parent SubjectMaps with the expression prefix
        var adaptedSubjectGenerators = refObjectMap.getParentTriplesMap().getSubjectMaps().stream()
                .map(sm -> sm.applyExpressionAdapter(expr -> prefix + expr))
                .map(rdfTermGeneratorFactory::getSubjectGenerator)
                .toList();

        // Collect all prefixed expressions to determine list size
        var prefixedExpressions = refObjectMap.getParentTriplesMap().getSubjectMaps().stream()
                .flatMap(sm -> sm.getExpressionMapExpressionSet().stream())
                .map(expr -> prefix + expr)
                .sorted()
                .toList();

        return (expressionEvaluation, datatypeMapper) -> {
            if (prefixedExpressions.isEmpty()) {
                return List.of();
            }

            // Determine list size from first prefixed expression
            var firstValues = expressionEvaluation
                    .apply(prefixedExpressions.get(0))
                    .map(ExpressionEvaluation::extractValues)
                    .orElse(List.of());

            int listSize = firstValues.size();
            if (listSize == 0) {
                return List.of();
            }

            // For each position, create a per-position evaluation and generate subjects
            var results = new ArrayList<MappedValue<Value>>();
            for (int i = 0; i < listSize; i++) {
                final int pos = i;
                // Create an expression evaluation that returns scalar value at position pos
                ExpressionEvaluation posEval =
                        expr -> expressionEvaluation.apply(expr).flatMap(obj -> {
                            var vals = ExpressionEvaluation.extractValues(obj);
                            return pos < vals.size() ? Optional.of(vals.get(pos)) : Optional.empty();
                        });

                for (var gen : adaptedSubjectGenerators) {
                    gen.apply(posEval, datatypeMapper).stream()
                            .map(mv -> (MappedValue<Value>) (MappedValue<?>) mv)
                            .forEach(results::add);
                }
            }
            return results;
        };
    }

    private List<MappedValue<Value>> handleEmpty() {
        if (gatherMap.getAllowEmptyListAndContainer()) {
            if (gatherMap.getGatherAs().equals(RDF.LIST)) {
                return List.of(RdfMappedValue.of(RDF.NIL, gatherMap.getTargets()));
            }
            return List.of(RdfContainer.empty(gatherMap.getGatherAs(), valueFactory, gatherMap.getTargets()));
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
