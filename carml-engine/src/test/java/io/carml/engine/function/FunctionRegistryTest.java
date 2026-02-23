package io.carml.engine.function;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.model.IRI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FunctionRegistryTest {

    private static final IRI FUNC_IRI_1 = iri("http://example.org/func1");

    private static final IRI FUNC_IRI_2 = iri("http://example.org/func2");

    private static final IRI UNKNOWN_IRI = iri("http://example.org/unknown");

    private FunctionRegistry registry;

    @BeforeEach
    void setUp() {
        registry = FunctionRegistry.create();
    }

    @Test
    void getFunction_returnsDescriptor_afterRegister() {
        var descriptor = stubDescriptor(FUNC_IRI_1);

        registry.register(descriptor);

        assertThat(registry.getFunction(FUNC_IRI_1).isPresent(), is(true));
        assertThat(registry.getFunction(FUNC_IRI_1).get(), is(descriptor));
    }

    @Test
    void register_replacesExistingDescriptor_withSameIri() {
        var first = stubDescriptor(FUNC_IRI_1);
        var second = stubDescriptor(FUNC_IRI_1);

        registry.register(first);
        registry.register(second);

        assertThat(registry.getFunction(FUNC_IRI_1).get(), is(second));
        assertThat(registry.getRegisteredFunctions(), hasSize(1));
    }

    @Test
    void getFunction_returnsEmpty_forUnknownIri() {
        assertThat(registry.getFunction(UNKNOWN_IRI).isEmpty(), is(true));
    }

    @Test
    void unregister_removesFunction() {
        registry.register(stubDescriptor(FUNC_IRI_1));

        registry.unregister(FUNC_IRI_1);

        assertThat(registry.getFunction(FUNC_IRI_1).isEmpty(), is(true));
    }

    @Test
    void unregister_noOp_forUnknownIri() {
        registry.unregister(UNKNOWN_IRI);

        assertThat(registry.getRegisteredFunctions(), is(empty()));
    }

    @Test
    void getRegisteredFunctions_returnsCorrectIris() {
        registry.register(stubDescriptor(FUNC_IRI_1));
        registry.register(stubDescriptor(FUNC_IRI_2));

        assertThat(registry.getRegisteredFunctions(), containsInAnyOrder(FUNC_IRI_1, FUNC_IRI_2));
    }

    @Test
    void registerAll_registersAllFromProvider() {
        var desc1 = stubDescriptor(FUNC_IRI_1);
        var desc2 = stubDescriptor(FUNC_IRI_2);
        FunctionProvider provider = () -> List.of(desc1, desc2);

        registry.registerAll(provider);

        assertThat(registry.getRegisteredFunctions(), containsInAnyOrder(FUNC_IRI_1, FUNC_IRI_2));
    }

    @Test
    void register_throwsIllegalArgument_givenNull() {
        assertThrows(IllegalArgumentException.class, () -> registry.register(null));
    }

    @Test
    void register_throwsIllegalArgument_givenDescriptorWithNullIri() {
        var descriptor = new FunctionDescriptor() {
            @Override
            public IRI getFunctionIri() {
                return null;
            }

            @Override
            public List<ParameterDescriptor> getParameters() {
                return List.of();
            }

            @Override
            public List<ReturnDescriptor> getReturns() {
                return List.of();
            }

            @Override
            public Object execute(Map<IRI, Object> parameterValues) {
                return null;
            }
        };

        assertThrows(IllegalArgumentException.class, () -> registry.register(descriptor));
    }

    private static FunctionDescriptor stubDescriptor(IRI functionIri) {
        return new FunctionDescriptor() {
            @Override
            public IRI getFunctionIri() {
                return functionIri;
            }

            @Override
            public List<ParameterDescriptor> getParameters() {
                return List.of();
            }

            @Override
            public List<ReturnDescriptor> getReturns() {
                return List.of();
            }

            @Override
            public Object execute(Map<IRI, Object> parameterValues) {
                return null;
            }
        };
    }
}
