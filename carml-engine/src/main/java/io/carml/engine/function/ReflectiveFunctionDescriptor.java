package io.carml.engine.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.model.IRI;

/**
 * A {@link FunctionDescriptor} that invokes a Java method on a target object via reflection.
 *
 * <p>Parameter binding is positional: the parameter IRI list determines the order of arguments
 * passed to the underlying method.
 */
final class ReflectiveFunctionDescriptor implements FunctionDescriptor {

    private final IRI functionIri;
    private final List<ParameterDescriptor> parameters;
    private final List<ReturnDescriptor> returns;
    private final Object target;
    private final Method method;
    private final List<IRI> paramIris;

    ReflectiveFunctionDescriptor(
            IRI functionIri,
            List<ParameterDescriptor> parameters,
            List<ReturnDescriptor> returns,
            Object target,
            Method method,
            List<IRI> paramIris) {
        this.functionIri = functionIri;
        this.parameters = parameters;
        this.returns = returns;
        this.target = target;
        this.method = method;
        this.paramIris = paramIris;
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
        var args = new Object[paramIris.size()];
        for (int i = 0; i < paramIris.size(); i++) {
            // Note: if parameterValues does not contain an entry for a parameter IRI,
            // Map.get() returns null, which is passed as the method argument.
            args[i] = parameterValues.get(paramIris.get(i));
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
