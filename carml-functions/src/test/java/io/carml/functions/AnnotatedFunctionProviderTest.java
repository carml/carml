package io.carml.functions;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.eclipse.rdf4j.model.IRI;
import org.junit.jupiter.api.Test;

class AnnotatedFunctionProviderTest {

    private static final IRI UPPER_IRI = iri("http://example.org/toUpperCase");
    private static final IRI INPUT_IRI = iri("http://example.org/input");
    private static final IRI CONCAT_IRI = iri("http://example.org/concat");
    private static final IRI LEFT_IRI = iri("http://example.org/left");
    private static final IRI RIGHT_IRI = iri("http://example.org/right");
    private static final IRI NOOP_IRI = iri("http://example.org/noOp");

    // -- Test fixtures using @RmlFunction / @RmlParam --

    @SuppressWarnings("unused") // methods are discovered reflectively by AnnotatedFunctionProvider
    static class RmlAnnotatedFunctions {

        @RmlFunction("http://example.org/toUpperCase")
        public String toUpperCase(@RmlParam("http://example.org/input") String input) {
            return input.toUpperCase();
        }

        @RmlFunction("http://example.org/concat")
        public String concat(
                @RmlParam("http://example.org/left") String left, @RmlParam("http://example.org/right") String right) {
            return left + right;
        }

        @RmlFunction("http://example.org/noOp")
        public void noOp() {
            // no-op
        }
    }

    // -- Test fixtures using @FnoFunction / @FnoParam (legacy) --

    @SuppressWarnings({"deprecation", "unused"}) // methods are discovered reflectively
    static class FnoAnnotatedFunctions {

        @FnoFunction("http://example.org/toUpperCase")
        public String toUpperCase(@FnoParam("http://example.org/input") String input) {
            return input.toUpperCase();
        }
    }

    // -- Test fixture with both annotations (precedence test) --

    @SuppressWarnings({"deprecation", "unused"}) // methods are discovered reflectively
    static class DualAnnotatedFunctions {

        @RmlFunction("http://example.org/rmlIri")
        @FnoFunction("http://example.org/fnoIri")
        public String dual(
                @RmlParam("http://example.org/rmlParam") @FnoParam("http://example.org/fnoParam") String input) {
            return input;
        }
    }

    // -- Test fixture with throwing method --

    @SuppressWarnings("unused") // methods are discovered reflectively
    static class ThrowingFunctions {

        @RmlFunction("http://example.org/throws")
        public String throwsException() {
            throw new RuntimeException("user error");
        }
    }

    // -- Test fixture with missing param annotation --

    @SuppressWarnings("unused") // methods are discovered reflectively
    static class MissingParamAnnotation {

        @RmlFunction("http://example.org/bad")
        public String bad(String input) {
            return input;
        }
    }

    // -- Test fixture with no annotated methods --

    @SuppressWarnings("unused") // methods are discovered reflectively
    static class NoAnnotatedMethods {

        public String notAFunction(String input) {
            return input;
        }
    }

    // -- Tests --

    @Test
    void getFunctions_scansRmlFunctionMethods() {
        var provider = new AnnotatedFunctionProvider(new RmlAnnotatedFunctions());

        var functions = provider.getFunctions();

        assertThat(functions, hasSize(3));
    }

    @Test
    void getFunctions_scansFnoFunctionMethods() {
        var provider = new AnnotatedFunctionProvider(new FnoAnnotatedFunctions());

        var functions = provider.getFunctions();

        assertThat(functions, hasSize(1));
    }

    @Test
    void getFunctions_returnsEmpty_givenNoAnnotatedMethods() {
        var provider = new AnnotatedFunctionProvider(new NoAnnotatedMethods());

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void createDescriptor_setsCorrectFunctionIri_forRmlFunction() {
        var provider = new AnnotatedFunctionProvider(new RmlAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream()
                .filter(d -> d.getFunctionIri().equals(UPPER_IRI))
                .findFirst()
                .orElseThrow();

        assertThat(descriptor.getFunctionIri(), is(UPPER_IRI));
    }

    @Test
    void createDescriptor_setsCorrectFunctionIri_forFnoFunction() {
        var provider = new AnnotatedFunctionProvider(new FnoAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream()
                .filter(d -> d.getFunctionIri().equals(UPPER_IRI))
                .findFirst()
                .orElseThrow();

        assertThat(descriptor.getFunctionIri(), is(UPPER_IRI));
    }

    @Test
    void createDescriptor_createsParameterDescriptor_fromRmlParam() {
        var provider = new AnnotatedFunctionProvider(new RmlAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream()
                .filter(d -> d.getFunctionIri().equals(UPPER_IRI))
                .findFirst()
                .orElseThrow();

        assertThat(descriptor.getParameters(), hasSize(1));
        var param = descriptor.getParameters().get(0);
        assertThat(param.parameterIri(), is(INPUT_IRI));
        assertThat(param.type(), is(String.class));
        assertThat(param.required(), is(true));
    }

    @Test
    void createDescriptor_createsParameterDescriptor_fromFnoParam() {
        var provider = new AnnotatedFunctionProvider(new FnoAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream()
                .filter(d -> d.getFunctionIri().equals(UPPER_IRI))
                .findFirst()
                .orElseThrow();

        assertThat(descriptor.getParameters(), hasSize(1));
        var param = descriptor.getParameters().get(0);
        assertThat(param.parameterIri(), is(INPUT_IRI));
        assertThat(param.type(), is(String.class));
        assertThat(param.required(), is(true));
    }

    @Test
    void createDescriptor_createsReturnDescriptor_withNullIriAndCorrectType() {
        var provider = new AnnotatedFunctionProvider(new RmlAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream()
                .filter(d -> d.getFunctionIri().equals(UPPER_IRI))
                .findFirst()
                .orElseThrow();

        assertThat(descriptor.getReturns(), hasSize(1));
        var ret = descriptor.getReturns().get(0);
        assertThat(ret.outputIri(), is(nullValue()));
        assertThat(ret.type(), is(String.class));
    }

    @Test
    void execute_invokesMethodAndReturnsResult() {
        var provider = new AnnotatedFunctionProvider(new RmlAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream()
                .filter(d -> d.getFunctionIri().equals(UPPER_IRI))
                .findFirst()
                .orElseThrow();

        var result = descriptor.execute(Map.of(INPUT_IRI, "hello"));

        assertThat(result, is("HELLO"));
    }

    @Test
    void execute_multiParamFunction_invokesCorrectly() {
        var provider = new AnnotatedFunctionProvider(new RmlAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream()
                .filter(d -> d.getFunctionIri().equals(CONCAT_IRI))
                .findFirst()
                .orElseThrow();

        var result = descriptor.execute(Map.of(LEFT_IRI, "foo", RIGHT_IRI, "bar"));

        assertThat(result, is("foobar"));
    }

    @Test
    void execute_noParamFunction_invokesCorrectly() {
        var provider = new AnnotatedFunctionProvider(new RmlAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream()
                .filter(d -> d.getFunctionIri().equals(NOOP_IRI))
                .findFirst()
                .orElseThrow();

        assertThat(descriptor.getParameters(), is(empty()));

        var result = descriptor.execute(Map.of());

        assertThat(result, is(nullValue()));
    }

    @Test
    void constructor_throwsIllegalArgument_givenMissingParamAnnotation() {
        var target = new MissingParamAnnotation();
        assertThrows(IllegalArgumentException.class, () -> new AnnotatedFunctionProvider(target));
    }

    @Test
    void getFunctionIri_prefersRmlFunction_givenBothAnnotationsPresent() {
        var provider = new AnnotatedFunctionProvider(new DualAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream().findFirst().orElseThrow();

        assertThat(descriptor.getFunctionIri(), is(iri("http://example.org/rmlIri")));
    }

    @Test
    void getParamIri_prefersRmlParam_givenBothAnnotationsPresent() {
        var provider = new AnnotatedFunctionProvider(new DualAnnotatedFunctions());

        var descriptor = provider.getFunctions().stream().findFirst().orElseThrow();
        var param = descriptor.getParameters().get(0);

        assertThat(param.parameterIri(), is(iri("http://example.org/rmlParam")));
    }

    @Test
    void execute_wrapsUserMethodException_withOriginalCause() {
        var provider = new AnnotatedFunctionProvider(new ThrowingFunctions());
        var descriptor = provider.getFunctions().stream().findFirst().orElseThrow();

        var params = Map.<IRI, Object>of();
        var ex = assertThrows(FunctionInvocationException.class, () -> descriptor.execute(params));

        assertThat(ex.getCause().getMessage(), is("user error"));
    }

    @Test
    void getFunctions_scansMultipleObjects() {
        var provider = new AnnotatedFunctionProvider(new RmlAnnotatedFunctions(), new FnoAnnotatedFunctions());

        // RmlAnnotatedFunctions has 3, FnoAnnotatedFunctions has 1
        assertThat(provider.getFunctions(), hasSize(4));
    }
}
