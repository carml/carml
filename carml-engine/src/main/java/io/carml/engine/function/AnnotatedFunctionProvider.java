package io.carml.engine.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/** Wraps annotated objects into {@link FunctionDescriptor} instances via reflection. */
@SuppressWarnings("deprecation")
public class AnnotatedFunctionProvider implements FunctionProvider {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private final List<FunctionDescriptor> descriptors;

    public AnnotatedFunctionProvider(Object... functionObjects) {
        descriptors = Arrays.stream(functionObjects)
                .map(AnnotatedFunctionProvider::scan)
                .flatMap(Collection::stream)
                .toList();
    }

    @Override
    public Collection<FunctionDescriptor> getFunctions() {
        return descriptors;
    }

    private static List<FunctionDescriptor> scan(Object functionObject) {
        return Arrays.stream(functionObject.getClass().getMethods())
                .filter(AnnotatedFunctionProvider::hasAnnotation)
                .map(method -> createDescriptor(functionObject, method))
                .toList();
    }

    private static boolean hasAnnotation(Method method) {
        return method.isAnnotationPresent(RmlFunction.class) || method.isAnnotationPresent(FnoFunction.class);
    }

    private static String getFunctionIri(Method method) {
        var rmlFunction = method.getAnnotation(RmlFunction.class);
        if (rmlFunction != null) {
            return rmlFunction.value();
        }
        return method.getAnnotation(FnoFunction.class).value();
    }

    private static FunctionDescriptor createDescriptor(Object target, Method method) {
        var functionIri = VF.createIRI(getFunctionIri(method));

        var parameters = Arrays.stream(method.getParameters())
                .map(AnnotatedFunctionProvider::createParameterDescriptor)
                .toList();

        var paramIris = parameters.stream().map(ParameterDescriptor::iri).toList();

        var returns = List.of(new ReturnDescriptor(null, method.getReturnType()));

        return new ReflectiveFunctionDescriptor(functionIri, parameters, returns, target, method, paramIris);
    }

    private static ParameterDescriptor createParameterDescriptor(Parameter parameter) {
        var paramIri = getParamIri(parameter);
        return new ParameterDescriptor(VF.createIRI(paramIri), parameter.getType(), true);
    }

    private static String getParamIri(Parameter parameter) {
        var rmlParam = parameter.getAnnotation(RmlParam.class);
        if (rmlParam != null) {
            return rmlParam.value();
        }

        var fnoParam = parameter.getAnnotation(FnoParam.class);
        if (fnoParam != null) {
            return fnoParam.value();
        }

        throw new IllegalArgumentException(String.format(
                "Parameter '%s' of method '%s' has neither @%s nor @%s annotation",
                parameter.getName(),
                parameter.getDeclaringExecutable().getName(),
                RmlParam.class.getSimpleName(),
                FnoParam.class.getSimpleName()));
    }

    private static final class ReflectiveFunctionDescriptor implements FunctionDescriptor {

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
                args[i] = parameterValues.get(paramIris.get(i));
            }

            try {
                return method.invoke(target, args);
            } catch (IllegalAccessException exception) {
                throw new IllegalStateException(
                        String.format("Failed to invoke function '%s'", functionIri), exception);
            } catch (InvocationTargetException exception) {
                throw new IllegalStateException(
                        String.format("Failed to invoke function '%s'", functionIri),
                        exception.getCause() != null ? exception.getCause() : exception);
            }
        }
    }
}
