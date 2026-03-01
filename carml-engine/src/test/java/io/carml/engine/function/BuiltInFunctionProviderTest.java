package io.carml.engine.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.vocab.Rdf.Grel;
import io.carml.vocab.Rdf.IdlabFn;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
        assertThat(findDescriptor(IdlabFn.isNull).getFunctionIri(), is(IdlabFn.isNull));
    }

    @Test
    void getFunctions_containsIsNotNull() {
        assertThat(findDescriptor(IdlabFn.isNotNull).getFunctionIri(), is(IdlabFn.isNotNull));
    }

    @Test
    void getFunctions_containsEquals() {
        assertThat(findDescriptor(IdlabFn.equal).getFunctionIri(), is(IdlabFn.equal));
    }

    @Test
    void getFunctions_containsNotEquals() {
        assertThat(findDescriptor(IdlabFn.notEqual).getFunctionIri(), is(IdlabFn.notEqual));
    }

    @Test
    void getFunctions_containsIf() {
        assertThat(findDescriptor(Grel.controls_if).getFunctionIri(), is(Grel.controls_if));
    }

    // -- isNull --

    @Test
    void isNull_hasOneOptionalParameter() {
        var descriptor = findDescriptor(IdlabFn.isNull);
        assertThat(descriptor.getParameters(), hasSize(1));
        assertThat(descriptor.getParameters().get(0).predicateIri(), is(IdlabFn.str));
        assertThat(descriptor.getParameters().get(0).required(), is(false));
    }

    @Test
    void isNull_returnsTrue_givenNull() {
        var descriptor = findDescriptor(IdlabFn.isNull);
        var params = new HashMap<IRI, Object>();
        params.put(IdlabFn.str, null);

        assertThat(descriptor.execute(params), is(true));
    }

    @Test
    void isNull_returnsFalse_givenNonNull() {
        var descriptor = findDescriptor(IdlabFn.isNull);

        assertThat(descriptor.execute(Map.of(IdlabFn.str, "hello")), is(false));
    }

    @Test
    void isNull_returnsTrue_givenMissingKey() {
        var descriptor = findDescriptor(IdlabFn.isNull);

        assertThat(descriptor.execute(Map.of()), is(true));
    }

    // -- isNotNull --

    @Test
    void isNotNull_hasOneOptionalParameter() {
        var descriptor = findDescriptor(IdlabFn.isNotNull);
        assertThat(descriptor.getParameters(), hasSize(1));
        assertThat(descriptor.getParameters().get(0).predicateIri(), is(IdlabFn.str));
        assertThat(descriptor.getParameters().get(0).required(), is(false));
    }

    @Test
    void isNotNull_returnsFalse_givenNull() {
        var descriptor = findDescriptor(IdlabFn.isNotNull);
        var params = new HashMap<IRI, Object>();
        params.put(IdlabFn.str, null);

        assertThat(descriptor.execute(params), is(false));
    }

    @Test
    void isNotNull_returnsTrue_givenNonNull() {
        var descriptor = findDescriptor(IdlabFn.isNotNull);

        assertThat(descriptor.execute(Map.of(IdlabFn.str, "hello")), is(true));
    }

    @Test
    void isNotNull_returnsFalse_givenMissingKey() {
        var descriptor = findDescriptor(IdlabFn.isNotNull);

        assertThat(descriptor.execute(Map.of()), is(false));
    }

    // -- equals --

    @Test
    void equals_hasTwoRequiredParameters() {
        var descriptor = findDescriptor(IdlabFn.equal);
        assertThat(descriptor.getParameters(), hasSize(2));
        assertThat(descriptor.getParameters().get(0).predicateIri(), is(Grel.valueParam));
        assertThat(descriptor.getParameters().get(0).required(), is(true));
        assertThat(descriptor.getParameters().get(1).predicateIri(), is(Grel.valueParam2));
        assertThat(descriptor.getParameters().get(1).required(), is(true));
    }

    @Test
    void equals_returnsTrue_givenEqualValues() {
        var descriptor = findDescriptor(IdlabFn.equal);

        assertThat(descriptor.execute(Map.of(Grel.valueParam, "abc", Grel.valueParam2, "abc")), is(true));
    }

    @Test
    void equals_returnsFalse_givenDifferentValues() {
        var descriptor = findDescriptor(IdlabFn.equal);

        assertThat(descriptor.execute(Map.of(Grel.valueParam, "abc", Grel.valueParam2, "xyz")), is(false));
    }

    @Test
    void equals_returnsTrue_givenBothNull() {
        var descriptor = findDescriptor(IdlabFn.equal);
        var params = new HashMap<IRI, Object>();
        params.put(Grel.valueParam, null);
        params.put(Grel.valueParam2, null);

        assertThat(descriptor.execute(params), is(true));
    }

    // -- notEquals --

    @Test
    void notEquals_hasTwoRequiredParameters() {
        var descriptor = findDescriptor(IdlabFn.notEqual);
        assertThat(descriptor.getParameters(), hasSize(2));
        assertThat(descriptor.getParameters().get(0).predicateIri(), is(Grel.valueParam));
        assertThat(descriptor.getParameters().get(0).required(), is(true));
        assertThat(descriptor.getParameters().get(1).predicateIri(), is(Grel.valueParam2));
        assertThat(descriptor.getParameters().get(1).required(), is(true));
    }

    @Test
    void notEquals_returnsFalse_givenEqualValues() {
        var descriptor = findDescriptor(IdlabFn.notEqual);

        assertThat(descriptor.execute(Map.of(Grel.valueParam, "abc", Grel.valueParam2, "abc")), is(false));
    }

    @Test
    void notEquals_returnsTrue_givenDifferentValues() {
        var descriptor = findDescriptor(IdlabFn.notEqual);

        assertThat(descriptor.execute(Map.of(Grel.valueParam, "abc", Grel.valueParam2, "xyz")), is(true));
    }

    @Test
    void notEquals_returnsFalse_givenBothNull() {
        var descriptor = findDescriptor(IdlabFn.notEqual);
        var params = new HashMap<IRI, Object>();
        params.put(Grel.valueParam, null);
        params.put(Grel.valueParam2, null);

        assertThat(descriptor.execute(params), is(false));
    }

    // -- IF --

    @Test
    void if_hasThreeParameters_firstRequiredOthersOptional() {
        var descriptor = findDescriptor(Grel.controls_if);
        assertThat(descriptor.getParameters(), hasSize(3));
        assertThat(descriptor.getParameters().get(0).predicateIri(), is(Grel.bool_b));
        assertThat(descriptor.getParameters().get(0).required(), is(true));
        assertThat(descriptor.getParameters().get(1).predicateIri(), is(Grel.any_true));
        assertThat(descriptor.getParameters().get(1).required(), is(false));
        assertThat(descriptor.getParameters().get(2).predicateIri(), is(Grel.any_false));
        assertThat(descriptor.getParameters().get(2).required(), is(false));
    }

    @Test
    void if_returnsTrueBranch_givenBooleanTrue() {
        var descriptor = findDescriptor(Grel.controls_if);

        var result = descriptor.execute(Map.of(Grel.bool_b, Boolean.TRUE, Grel.any_true, "yes", Grel.any_false, "no"));

        assertThat(result, is("yes"));
    }

    @Test
    void if_returnsFalseBranch_givenBooleanFalse() {
        var descriptor = findDescriptor(Grel.controls_if);

        var result = descriptor.execute(Map.of(Grel.bool_b, Boolean.FALSE, Grel.any_true, "yes", Grel.any_false, "no"));

        assertThat(result, is("no"));
    }

    static Stream<Arguments> stringConditionCases() {
        return Stream.of(Arguments.of("true", "yes"), Arguments.of("TRUE", "yes"), Arguments.of("false", "no"));
    }

    @ParameterizedTest
    @MethodSource("stringConditionCases")
    void if_returnsExpectedBranch_givenStringCondition(String condition, String expected) {
        var descriptor = findDescriptor(Grel.controls_if);

        var result = descriptor.execute(Map.of(Grel.bool_b, condition, Grel.any_true, "yes", Grel.any_false, "no"));

        assertThat(result, is(expected));
    }

    @Test
    void if_returnsFalseBranch_givenNonBooleanObject() {
        var descriptor = findDescriptor(Grel.controls_if);

        var result = descriptor.execute(Map.of(Grel.bool_b, 42, Grel.any_true, "yes", Grel.any_false, "no"));

        assertThat(result, is("no"));
    }

    @Test
    void if_returnsNull_givenTrueConditionAndMissingTrueBranch() {
        var descriptor = findDescriptor(Grel.controls_if);

        var result = descriptor.execute(Map.of(Grel.bool_b, Boolean.TRUE));

        assertThat(result, is(nullValue()));
    }

    @Test
    void if_returnsNull_givenFalseConditionAndMissingFalseBranch() {
        var descriptor = findDescriptor(Grel.controls_if);

        var result = descriptor.execute(Map.of(Grel.bool_b, Boolean.FALSE));

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
