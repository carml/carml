package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.impl.CarmlJoin;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RefObjectMapperTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    SimpleValueFactory vf = SimpleValueFactory.getInstance();

    @Mock
    ParentTriplesMapper<?> parentTriplesMapper;

    @Mock
    EvaluateExpression evaluate;

    RefObjectMapper refObjectMapper;

    @Before
    public void setup() {
        Set<Join> joinConditions = ImmutableSet.of(
            new CarmlJoin("childRef", "parentRef"),
            new CarmlJoin("nonExistingChildRef", "parentRef2"));
        refObjectMapper = new RefObjectMapper(parentTriplesMapper, joinConditions);
    }

    @Test
    public void test() {

        when(evaluate.apply("childRef")).thenReturn(Optional.of("child value"));
        when(evaluate.apply("nonExistingChildRef")).thenReturn(Optional.empty());
        Set<Pair<String, Object>> expectedJoinValues = ImmutableSet.of(
            Pair.of("parentRef", "child value"),
            Pair.of("parentRef2", emptyList()));
        Set<Resource> parentTriplesMappingResult = ImmutableSet.of(vf.createIRI("http://example.org/123"));
        when(parentTriplesMapper.map(expectedJoinValues)).thenReturn(parentTriplesMappingResult);

        Set<Resource> result = refObjectMapper.map(evaluate);

        assertThat(result, is(parentTriplesMappingResult));
        verify(evaluate).apply("childRef");
        verify(evaluate).apply("nonExistingChildRef");
        verify(parentTriplesMapper).map(expectedJoinValues);

    }
}
