package io.carml.functions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.model.IRI;

/**
 * A {@link FunctionDescriptor} that invokes a Java method on a target object via reflection.
 *
 * <p>An {@code argSlots} array maps Java argument positions to entries in the descriptor's
 * parameter list: {@code argSlots[i]} is the index into {@code parameters} whose value should be
 * passed at Java argument position {@code i}. The default identity mapping (used when no explicit
 * position binding exists) is built so that {@code parameters} order equals the Java method's
 * parameter order.
 *
 * <p>If {@code target} is {@code null}, the method is invoked statically.
 */
final class ReflectiveFunctionDescriptor implements FunctionDescriptor {

    private final IRI functionIri;
    private final List<ParameterDescriptor> parameters;
    private final List<ReturnDescriptor> returns;
    private final Object target;
    private final Method method;
    private final int[] argSlots;

    ReflectiveFunctionDescriptor(
            IRI functionIri,
            List<ParameterDescriptor> parameters,
            List<ReturnDescriptor> returns,
            Object target,
            Method method,
            int[] argSlots) {
        this.functionIri = functionIri;
        this.parameters = parameters;
        this.returns = returns;
        this.target = target;
        this.method = method;
        this.argSlots = argSlots;
    }

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
        var args = new Object[argSlots.length];
        for (int i = 0; i < argSlots.length; i++) {
            // Note: if parameterValues does not contain an entry for a parameter IRI,
            // Map.get() returns null, which is passed as the method argument.
            args[i] = parameterValues.get(parameters.get(argSlots[i]).parameterIri());
        }

        try {
            return method.invoke(target, args);
        } catch (IllegalAccessException exception) {
            throw new IllegalStateException("Failed to invoke function '%s'".formatted(functionIri), exception);
        } catch (InvocationTargetException exception) {
            throw new IllegalStateException(
                    "Failed to invoke function '%s'".formatted(functionIri),
                    exception.getCause() != null ? exception.getCause() : exception);
        }
    }
}
