package io.carml.engine.function;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/** Ships the 5 built-in condition functions required by the RML-FNML spec. */
public class BuiltInFunctionProvider implements FunctionProvider {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    static final String IDLAB_FN_NS = "https://w3id.org/imec/idlab/function#";

    static final String GREL_NS = "http://users.ugent.be/~bjdmeest/function/grel.ttl#";

    static final IRI IDLAB_FN_IS_NULL = VF.createIRI(IDLAB_FN_NS + "isNull");

    static final IRI IDLAB_FN_IS_NOT_NULL = VF.createIRI(IDLAB_FN_NS + "isNotNull");

    static final IRI IDLAB_FN_EQUAL = VF.createIRI(IDLAB_FN_NS + "equal");

    static final IRI IDLAB_FN_NOT_EQUAL = VF.createIRI(IDLAB_FN_NS + "notEqual");

    static final IRI GREL_CONTROLS_IF = VF.createIRI(GREL_NS + "controls_if");

    static final IRI IDLAB_FN_STR = VF.createIRI(IDLAB_FN_NS + "str");

    static final IRI GREL_VALUE_PARAM = VF.createIRI(GREL_NS + "valueParam");

    static final IRI GREL_VALUE_PARAM2 = VF.createIRI(GREL_NS + "valueParam2");

    static final IRI GREL_BOOL_B = VF.createIRI(GREL_NS + "bool_b");

    static final IRI GREL_ANY_TRUE = VF.createIRI(GREL_NS + "any_true");

    static final IRI GREL_ANY_FALSE = VF.createIRI(GREL_NS + "any_false");

    private final Collection<FunctionDescriptor> functions;

    public BuiltInFunctionProvider() {
        functions = List.of(
                isNullDescriptor(), isNotNullDescriptor(), equalsDescriptor(), notEqualsDescriptor(), ifDescriptor());
    }

    @Override
    public Collection<FunctionDescriptor> getFunctions() {
        return functions;
    }

    private static FunctionDescriptor isNullDescriptor() {
        return new SimpleFunctionDescriptor(
                IDLAB_FN_IS_NULL,
                List.of(new ParameterDescriptor(IDLAB_FN_STR, Object.class, false)),
                List.of(new ReturnDescriptor(null, Boolean.class)),
                params -> params.get(IDLAB_FN_STR) == null);
    }

    private static FunctionDescriptor isNotNullDescriptor() {
        return new SimpleFunctionDescriptor(
                IDLAB_FN_IS_NOT_NULL,
                List.of(new ParameterDescriptor(IDLAB_FN_STR, Object.class, false)),
                List.of(new ReturnDescriptor(null, Boolean.class)),
                params -> params.get(IDLAB_FN_STR) != null);
    }

    private static FunctionDescriptor equalsDescriptor() {
        return new SimpleFunctionDescriptor(
                IDLAB_FN_EQUAL,
                List.of(
                        new ParameterDescriptor(GREL_VALUE_PARAM, Object.class, true),
                        new ParameterDescriptor(GREL_VALUE_PARAM2, Object.class, true)),
                List.of(new ReturnDescriptor(null, Boolean.class)),
                params -> Objects.equals(params.get(GREL_VALUE_PARAM), params.get(GREL_VALUE_PARAM2)));
    }

    private static FunctionDescriptor notEqualsDescriptor() {
        return new SimpleFunctionDescriptor(
                IDLAB_FN_NOT_EQUAL,
                List.of(
                        new ParameterDescriptor(GREL_VALUE_PARAM, Object.class, true),
                        new ParameterDescriptor(GREL_VALUE_PARAM2, Object.class, true)),
                List.of(new ReturnDescriptor(null, Boolean.class)),
                params -> !Objects.equals(params.get(GREL_VALUE_PARAM), params.get(GREL_VALUE_PARAM2)));
    }

    private static FunctionDescriptor ifDescriptor() {
        return new SimpleFunctionDescriptor(
                GREL_CONTROLS_IF,
                List.of(
                        new ParameterDescriptor(GREL_BOOL_B, Object.class, true),
                        new ParameterDescriptor(GREL_ANY_TRUE, Object.class, false),
                        new ParameterDescriptor(GREL_ANY_FALSE, Object.class, false)),
                List.of(new ReturnDescriptor(null, Object.class)),
                params -> {
                    var condition = params.get(GREL_BOOL_B);
                    return isTruthy(condition) ? params.get(GREL_ANY_TRUE) : params.get(GREL_ANY_FALSE);
                });
    }

    private static boolean isTruthy(Object value) {
        if (Boolean.TRUE.equals(value)) {
            return true;
        }
        if (value instanceof String stringValue) {
            return "true".equalsIgnoreCase(stringValue);
        }
        return false;
    }

    private record SimpleFunctionDescriptor(
            IRI functionIri,
            List<ParameterDescriptor> parameters,
            List<ReturnDescriptor> returns,
            Function<Map<IRI, Object>, Object> executor)
            implements FunctionDescriptor {

        @Override
        public IRI getFunctionIri() {
            return functionIri;
        }

        @Override
        public List<ParameterDescriptor> getParameters() {
            return parameters;
        }

        @Override
        public List<ReturnDescriptor> getReturns() {
            return returns;
        }

        @Override
        public Object execute(Map<IRI, Object> parameterValues) {
            return executor.apply(parameterValues);
        }
    }
}
