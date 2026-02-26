package io.carml.engine.function;

import io.carml.vocab.Rdf.Grel;
import io.carml.vocab.Rdf.IdlabFn;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.eclipse.rdf4j.model.IRI;

/** Ships the 5 built-in condition functions required by the RML-FNML spec. */
public class BuiltInFunctionProvider implements FunctionProvider {

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
                IdlabFn.isNull,
                List.of(new ParameterDescriptor(IdlabFn.str, Object.class, false)),
                List.of(new ReturnDescriptor(null, Boolean.class)),
                params -> params.get(IdlabFn.str) == null);
    }

    private static FunctionDescriptor isNotNullDescriptor() {
        return new SimpleFunctionDescriptor(
                IdlabFn.isNotNull,
                List.of(new ParameterDescriptor(IdlabFn.str, Object.class, false)),
                List.of(new ReturnDescriptor(null, Boolean.class)),
                params -> params.get(IdlabFn.str) != null);
    }

    private static FunctionDescriptor equalsDescriptor() {
        return new SimpleFunctionDescriptor(
                IdlabFn.equal,
                List.of(
                        new ParameterDescriptor(Grel.valueParam, Object.class, true),
                        new ParameterDescriptor(Grel.valueParam2, Object.class, true)),
                List.of(new ReturnDescriptor(null, Boolean.class)),
                params -> Objects.equals(params.get(Grel.valueParam), params.get(Grel.valueParam2)));
    }

    private static FunctionDescriptor notEqualsDescriptor() {
        return new SimpleFunctionDescriptor(
                IdlabFn.notEqual,
                List.of(
                        new ParameterDescriptor(Grel.valueParam, Object.class, true),
                        new ParameterDescriptor(Grel.valueParam2, Object.class, true)),
                List.of(new ReturnDescriptor(null, Boolean.class)),
                params -> !Objects.equals(params.get(Grel.valueParam), params.get(Grel.valueParam2)));
    }

    private static FunctionDescriptor ifDescriptor() {
        return new SimpleFunctionDescriptor(
                Grel.controls_if,
                List.of(
                        new ParameterDescriptor(Grel.bool_b, Object.class, true),
                        new ParameterDescriptor(Grel.any_true, Object.class, false),
                        new ParameterDescriptor(Grel.any_false, Object.class, false)),
                List.of(new ReturnDescriptor(null, Object.class)),
                params -> {
                    var condition = params.get(Grel.bool_b);
                    return isTruthy(condition) ? params.get(Grel.any_true) : params.get(Grel.any_false);
                });
    }

    public static boolean isTruthy(Object value) {
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
