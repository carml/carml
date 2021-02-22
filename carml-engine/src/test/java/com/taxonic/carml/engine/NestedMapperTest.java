package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.ContextEntry;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NestedMapperTest {

    ValueFactory vf = SimpleValueFactory.getInstance();

    @Mock
    ContextTriplesMapper<String> triplesMapper;

    @Mock
    Set<ContextEntry> contextEntries;

    @Mock
    LogicalSourceResolver.CreateContextEvaluate createContextEvaluate;

    @Captor
    ArgumentCaptor<EvaluateExpression> evaluateExpressionArgumentCaptor;

    NestedMapper<String> nestedMapper;

    @Before
    public void init() {
        nestedMapper = new NestedMapper<>(triplesMapper, contextEntries, createContextEvaluate);
    }

    @Test
    public void testMapCallsTriplesMapperWithCreatedContext() {

        EvaluateExpression evaluate = mock(EvaluateExpression.class);
        EvaluateExpression contextEvaluate = mock(EvaluateExpression.class);
        Model model = mock(Model.class);
        Set<Resource> resources = ImmutableSet.of(vf.createIRI("http://xyz.com/abc"), vf.createIRI("http://def.com/ghi"));

        when(contextEntries.isEmpty()).thenReturn(false);
        when(createContextEvaluate.apply(contextEntries, evaluate)).thenReturn(contextEvaluate);
        when(triplesMapper.map(model, contextEvaluate)).thenReturn(resources);

        Set<Resource> result = nestedMapper.map(model, evaluate);
        assertThat(result, is(resources));

        verify(createContextEvaluate).apply(contextEntries, evaluate);
        verify(triplesMapper).map(eq(model), evaluateExpressionArgumentCaptor.capture());
        assertThat(evaluateExpressionArgumentCaptor.getValue(), is(contextEvaluate));

    }

    @Test
    public void testUsesExistingEvaluateWhenNoContextEntries() {

        EvaluateExpression evaluate = mock(EvaluateExpression.class);
        Model model = mock(Model.class);
        Set<Resource> resources = ImmutableSet.of(vf.createIRI("http://xyz.com/abc"), vf.createIRI("http://def.com/ghi"));

        when(contextEntries.isEmpty()).thenReturn(true);
        when(triplesMapper.map(model, evaluate)).thenReturn(resources);

        Set<Resource> result = nestedMapper.map(model, evaluate);
        assertThat(result, is(resources));

        verifyNoInteractions(createContextEvaluate);
        verify(triplesMapper).map(eq(model), evaluateExpressionArgumentCaptor.capture());
        assertThat(evaluateExpressionArgumentCaptor.getValue(), is(evaluate));

    }
}