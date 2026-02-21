package io.carml.model.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.carml.model.ExpressionField;
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.NotNullAnnotation;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.Resource;
import io.carml.model.TriplesMap;
import io.carml.util.RmlMappingLoader;
import java.io.InputStream;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TriplesMapRdfSerializeTest {

    private RmlMappingLoader mappingLoader;

    @BeforeEach
    public void init() {
        mappingLoader = RmlMappingLoader.build();
    }

    @Test
    void triplesMapAsRdfRoundTripTest() {
        InputStream mappingSource = TriplesMapRdfSerializeTest.class.getResourceAsStream("Mapping.rml.ttl");
        Set<TriplesMap> mapping = mappingLoader.load(RDFFormat.TURTLE, mappingSource);

        Model model =
                mapping.stream().map(Resource::asRdf).flatMap(Model::stream).collect(ModelCollector.toModel());

        Set<TriplesMap> mappingReloaded = mappingLoader.load(model);

        Model modelReloaded = mappingReloaded.stream()
                .map(Resource::asRdf)
                .flatMap(Model::stream)
                .collect(ModelCollector.toModel());

        assertThat(model, is(modelReloaded));
    }

    @Test
    void expandedMappingRoundTripTest() {
        InputStream mappingSource = TriplesMapRdfSerializeTest.class.getResourceAsStream("ExpandedMapping.rml.ttl");
        Set<TriplesMap> mapping = mappingLoader.load(RDFFormat.TURTLE, mappingSource);

        Model model =
                mapping.stream().map(Resource::asRdf).flatMap(Model::stream).collect(ModelCollector.toModel());

        Set<TriplesMap> mappingReloaded = mappingLoader.load(model);

        Model modelReloaded = mappingReloaded.stream()
                .map(Resource::asRdf)
                .flatMap(Model::stream)
                .collect(ModelCollector.toModel());

        assertThat(model, is(modelReloaded));
    }

    @Test
    void logicalViewMappingRoundTripTest() {
        InputStream mappingSource = TriplesMapRdfSerializeTest.class.getResourceAsStream("LogicalViewMapping.rml.ttl");
        Set<TriplesMap> mapping = mappingLoader.load(RDFFormat.TURTLE, mappingSource);

        Model model =
                mapping.stream().map(Resource::asRdf).flatMap(Model::stream).collect(ModelCollector.toModel());

        Set<TriplesMap> mappingReloaded = mappingLoader.load(model);

        Model modelReloaded = mappingReloaded.stream()
                .map(Resource::asRdf)
                .flatMap(Model::stream)
                .collect(ModelCollector.toModel());

        assertThat(model, is(modelReloaded));
    }

    @Test
    void logicalViewMappingDeserializationTest() {
        InputStream mappingSource = TriplesMapRdfSerializeTest.class.getResourceAsStream("LogicalViewMapping.rml.ttl");
        Set<TriplesMap> mapping = mappingLoader.load(RDFFormat.TURTLE, mappingSource);

        // Find the PersonMapping TriplesMap — its logical source should be a LogicalView
        var personMapping = mapping.stream()
                .filter(tm -> tm.getResourceName().equals("http://example.org/PersonMapping"))
                .findFirst()
                .orElseThrow();

        assertThat(personMapping.getLogicalSource(), instanceOf(LogicalView.class));

        var personView = (LogicalView) personMapping.getLogicalSource();

        // viewOn should be a LogicalSource
        assertThat(personView.getViewOn(), instanceOf(LogicalSource.class));

        // 3 top-level fields: id, fullName, item
        assertThat(personView.getFields(), hasSize(3));

        long expressionFieldCount = personView.getFields().stream()
                .filter(ExpressionField.class::isInstance)
                .count();
        long iterableFieldCount = personView.getFields().stream()
                .filter(IterableField.class::isInstance)
                .count();
        assertThat(expressionFieldCount, is(2L));
        assertThat(iterableFieldCount, is(1L));

        // IterableField has 2 nested ExpressionFields
        var iterableField = personView.getFields().stream()
                .filter(IterableField.class::isInstance)
                .map(IterableField.class::cast)
                .findFirst()
                .orElseThrow();
        assertThat(iterableField.getFieldName(), is("item"));
        assertThat(iterableField.getIterator(), is("$.items[*]"));
        assertThat(iterableField.getFields(), hasSize(2));
        assertThat(iterableField.getFields().stream().allMatch(ExpressionField.class::isInstance), is(true));

        // 1 left join
        assertThat(personView.getLeftJoins(), hasSize(1));
        var leftJoin = personView.getLeftJoins().iterator().next();
        assertThat(leftJoin.getParentLogicalView(), notNullValue());
        assertThat(leftJoin.getJoinConditions(), hasSize(1));
        assertThat(leftJoin.getFields(), hasSize(1));

        // 2 structural annotations: PrimaryKey and NotNull
        assertThat(personView.getStructuralAnnotations(), hasSize(2));
        long pkCount = personView.getStructuralAnnotations().stream()
                .filter(PrimaryKeyAnnotation.class::isInstance)
                .count();
        long notNullCount = personView.getStructuralAnnotations().stream()
                .filter(NotNullAnnotation.class::isInstance)
                .count();
        assertThat(pkCount, is(1L));
        assertThat(notNullCount, is(1L));

        // AddressMapping should also have a LogicalView as logical source
        var addressMapping = mapping.stream()
                .filter(tm -> tm.getResourceName().equals("http://example.org/AddressMapping"))
                .findFirst()
                .orElseThrow();
        assertThat(addressMapping.getLogicalSource(), instanceOf(LogicalView.class));
    }
}
