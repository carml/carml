package io.carml.engine.function;

import static io.carml.engine.function.BuiltInFunctionProvider.GREL_ANY_FALSE;
import static io.carml.engine.function.BuiltInFunctionProvider.GREL_ANY_TRUE;
import static io.carml.engine.function.BuiltInFunctionProvider.GREL_BOOL_B;
import static io.carml.engine.function.BuiltInFunctionProvider.GREL_CONTROLS_IF;
import static io.carml.engine.function.BuiltInFunctionProvider.GREL_VALUE_PARAM;
import static io.carml.engine.function.BuiltInFunctionProvider.GREL_VALUE_PARAM2;
import static io.carml.engine.function.BuiltInFunctionProvider.IDLAB_FN_EQUAL;
import static io.carml.engine.function.BuiltInFunctionProvider.IDLAB_FN_IS_NOT_NULL;
import static io.carml.engine.function.BuiltInFunctionProvider.IDLAB_FN_IS_NULL;
import static io.carml.engine.function.BuiltInFunctionProvider.IDLAB_FN_NOT_EQUAL;
import static io.carml.engine.function.BuiltInFunctionProvider.IDLAB_FN_STR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.rdf4j.model.IRI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BuiltInFunctionProviderTest {

    private BuiltInFunctionProvider provider;

    @BeforeEach
    void setUp() {
        provider = new BuiltInFunctionProvider();
    }

    @Test
    void getFunctions_returnsExactly5Functions() {
        assertThat(provider.getFunctions(), hasSize(5));
    }

    @Test
    void getFunctions_containsIsNull() {
        assertThat(findDescriptor(IDLAB_FN_IS_NULL).getFunctionIri(), is(IDLAB_FN_IS_NULL));
    }

    @Test
    void getFunctions_containsIsNotNull() {
        assertThat(findDescriptor(IDLAB_FN_IS_NOT_NULL).getFunctionIri(), is(IDLAB_FN_IS_NOT_NULL));
    }

    @Test
    void getFunctions_containsEquals() {
        assertThat(findDescriptor(IDLAB_FN_EQUAL).getFunctionIri(), is(IDLAB_FN_EQUAL));
    }

    @Test
    void getFunctions_containsNotEquals() {
        assertThat(findDescriptor(IDLAB_FN_NOT_EQUAL).getFunctionIri(), is(IDLAB_FN_NOT_EQUAL));
    }

    @Test
    void getFunctions_containsIf() {
        assertThat(findDescriptor(GREL_CONTROLS_IF).getFunctionIri(), is(GREL_CONTROLS_IF));
    }

    // -- isNull --

    @Test
    void isNull_hasOneOptionalParameter() {
        var descriptor = findDescriptor(IDLAB_FN_IS_NULL);
        assertThat(descriptor.getParameters(), hasSize(1));
        assertThat(descriptor.getParameters().get(0).iri(), is(IDLAB_FN_STR));
        assertThat(descriptor.getParameters().get(0).required(), is(false));
    }

    @Test
    void isNull_returnsTrue_givenNull() {
        var descriptor = findDescriptor(IDLAB_FN_IS_NULL);
        var params = new HashMap<IRI, Object>();
        params.put(IDLAB_FN_STR, null);

        assertThat(descriptor.execute(params), is(true));
    }

    @Test
    void isNull_returnsFalse_givenNonNull() {
        var descriptor = findDescriptor(IDLAB_FN_IS_NULL);

        assertThat(descriptor.execute(Map.of(IDLAB_FN_STR, "hello")), is(false));
    }

    @Test
    void isNull_returnsTrue_givenMissingKey() {
        var descriptor = findDescriptor(IDLAB_FN_IS_NULL);

        assertThat(descriptor.execute(Map.of()), is(true));
    }

    // -- isNotNull --

    @Test
    void isNotNull_hasOneOptionalParameter() {
        var descriptor = findDescriptor(IDLAB_FN_IS_NOT_NULL);
        assertThat(descriptor.getParameters(), hasSize(1));
        assertThat(descriptor.getParameters().get(0).iri(), is(IDLAB_FN_STR));
        assertThat(descriptor.getParameters().get(0).required(), is(false));
    }

    @Test
    void isNotNull_returnsFalse_givenNull() {
        var descriptor = findDescriptor(IDLAB_FN_IS_NOT_NULL);
        var params = new HashMap<IRI, Object>();
        params.put(IDLAB_FN_STR, null);

        assertThat(descriptor.execute(params), is(false));
    }

    @Test
    void isNotNull_returnsTrue_givenNonNull() {
        var descriptor = findDescriptor(IDLAB_FN_IS_NOT_NULL);

        assertThat(descriptor.execute(Map.of(IDLAB_FN_STR, "hello")), is(true));
    }

    @Test
    void isNotNull_returnsFalse_givenMissingKey() {
        var descriptor = findDescriptor(IDLAB_FN_IS_NOT_NULL);

        assertThat(descriptor.execute(Map.of()), is(false));
    }

    // -- equals --

    @Test
    void equals_hasTwoRequiredParameters() {
        var descriptor = findDescriptor(IDLAB_FN_EQUAL);
        assertThat(descriptor.getParameters(), hasSize(2));
        assertThat(descriptor.getParameters().get(0).iri(), is(GREL_VALUE_PARAM));
        assertThat(descriptor.getParameters().get(0).required(), is(true));
        assertThat(descriptor.getParameters().get(1).iri(), is(GREL_VALUE_PARAM2));
        assertThat(descriptor.getParameters().get(1).required(), is(true));
    }

    @Test
    void equals_returnsTrue_givenEqualValues() {
        var descriptor = findDescriptor(IDLAB_FN_EQUAL);

        assertThat(descriptor.execute(Map.of(GREL_VALUE_PARAM, "abc", GREL_VALUE_PARAM2, "abc")), is(true));
    }

    @Test
    void equals_returnsFalse_givenDifferentValues() {
        var descriptor = findDescriptor(IDLAB_FN_EQUAL);

        assertThat(descriptor.execute(Map.of(GREL_VALUE_PARAM, "abc", GREL_VALUE_PARAM2, "xyz")), is(false));
    }

    @Test
    void equals_returnsTrue_givenBothNull() {
        var descriptor = findDescriptor(IDLAB_FN_EQUAL);
        var params = new HashMap<IRI, Object>();
        params.put(GREL_VALUE_PARAM, null);
        params.put(GREL_VALUE_PARAM2, null);

        assertThat(descriptor.execute(params), is(true));
    }

    // -- notEquals --

    @Test
    void notEquals_hasTwoRequiredParameters() {
        var descriptor = findDescriptor(IDLAB_FN_NOT_EQUAL);
        assertThat(descriptor.getParameters(), hasSize(2));
        assertThat(descriptor.getParameters().get(0).iri(), is(GREL_VALUE_PARAM));
        assertThat(descriptor.getParameters().get(0).required(), is(true));
        assertThat(descriptor.getParameters().get(1).iri(), is(GREL_VALUE_PARAM2));
        assertThat(descriptor.getParameters().get(1).required(), is(true));
    }

    @Test
    void notEquals_returnsFalse_givenEqualValues() {
        var descriptor = findDescriptor(IDLAB_FN_NOT_EQUAL);

        assertThat(descriptor.execute(Map.of(GREL_VALUE_PARAM, "abc", GREL_VALUE_PARAM2, "abc")), is(false));
    }

    @Test
    void notEquals_returnsTrue_givenDifferentValues() {
        var descriptor = findDescriptor(IDLAB_FN_NOT_EQUAL);

        assertThat(descriptor.execute(Map.of(GREL_VALUE_PARAM, "abc", GREL_VALUE_PARAM2, "xyz")), is(true));
    }

    @Test
    void notEquals_returnsFalse_givenBothNull() {
        var descriptor = findDescriptor(IDLAB_FN_NOT_EQUAL);
        var params = new HashMap<IRI, Object>();
        params.put(GREL_VALUE_PARAM, null);
        params.put(GREL_VALUE_PARAM2, null);

        assertThat(descriptor.execute(params), is(false));
    }

    // -- IF --

    @Test
    void if_hasThreeParameters_firstRequiredOthersOptional() {
        var descriptor = findDescriptor(GREL_CONTROLS_IF);
        assertThat(descriptor.getParameters(), hasSize(3));
        assertThat(descriptor.getParameters().get(0).iri(), is(GREL_BOOL_B));
        assertThat(descriptor.getParameters().get(0).required(), is(true));
        assertThat(descriptor.getParameters().get(1).iri(), is(GREL_ANY_TRUE));
        assertThat(descriptor.getParameters().get(1).required(), is(false));
        assertThat(descriptor.getParameters().get(2).iri(), is(GREL_ANY_FALSE));
        assertThat(descriptor.getParameters().get(2).required(), is(false));
    }

    @Test
    void if_returnsTrueBranch_givenBooleanTrue() {
        var descriptor = findDescriptor(GREL_CONTROLS_IF);

        var result = descriptor.execute(Map.of(GREL_BOOL_B, Boolean.TRUE, GREL_ANY_TRUE, "yes", GREL_ANY_FALSE, "no"));

        assertThat(result, is("yes"));
    }

    @Test
    void if_returnsFalseBranch_givenBooleanFalse() {
        var descriptor = findDescriptor(GREL_CONTROLS_IF);

        var result = descriptor.execute(Map.of(GREL_BOOL_B, Boolean.FALSE, GREL_ANY_TRUE, "yes", GREL_ANY_FALSE, "no"));

        assertThat(result, is("no"));
    }

    @Test
    void if_returnsTrueBranch_givenStringTrue() {
        var descriptor = findDescriptor(GREL_CONTROLS_IF);

        var result = descriptor.execute(Map.of(GREL_BOOL_B, "true", GREL_ANY_TRUE, "yes", GREL_ANY_FALSE, "no"));

        assertThat(result, is("yes"));
    }

    @Test
    void if_returnsTrueBranch_givenStringTrueCaseInsensitive() {
        var descriptor = findDescriptor(GREL_CONTROLS_IF);

        var result = descriptor.execute(Map.of(GREL_BOOL_B, "TRUE", GREL_ANY_TRUE, "yes", GREL_ANY_FALSE, "no"));

        assertThat(result, is("yes"));
    }

    @Test
    void if_returnsFalseBranch_givenStringFalse() {
        var descriptor = findDescriptor(GREL_CONTROLS_IF);

        var result = descriptor.execute(Map.of(GREL_BOOL_B, "false", GREL_ANY_TRUE, "yes", GREL_ANY_FALSE, "no"));

        assertThat(result, is("no"));
    }

    @Test
    void if_returnsFalseBranch_givenNonBooleanObject() {
        var descriptor = findDescriptor(GREL_CONTROLS_IF);

        var result = descriptor.execute(Map.of(GREL_BOOL_B, 42, GREL_ANY_TRUE, "yes", GREL_ANY_FALSE, "no"));

        assertThat(result, is("no"));
    }

    @Test
    void if_returnsNull_givenTrueConditionAndMissingTrueBranch() {
        var descriptor = findDescriptor(GREL_CONTROLS_IF);

        var result = descriptor.execute(Map.of(GREL_BOOL_B, Boolean.TRUE));

        assertThat(result, is(nullValue()));
    }

    @Test
    void if_returnsNull_givenFalseConditionAndMissingFalseBranch() {
        var descriptor = findDescriptor(GREL_CONTROLS_IF);

        var result = descriptor.execute(Map.of(GREL_BOOL_B, Boolean.FALSE));

        assertThat(result, is(nullValue()));
    }

    // -- Immutability --

    @Test
    void getFunctions_returnsUnmodifiableCollection() {
        Collection<FunctionDescriptor> functions = provider.getFunctions();

        assertThrows(UnsupportedOperationException.class, () -> functions.add(null));
    }

    private FunctionDescriptor findDescriptor(IRI functionIri) {
        return provider.getFunctions().stream()
                .filter(d -> d.getFunctionIri().equals(functionIri))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No function found with IRI: " + functionIri));
    }
}
