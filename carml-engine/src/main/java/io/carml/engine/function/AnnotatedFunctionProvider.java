package io.carml.engine.function;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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

        var paramIris =
                parameters.stream().map(ParameterDescriptor::parameterIri).toList();

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

        throw new IllegalArgumentException("Parameter '%s' of method '%s' has neither @%s nor @%s annotation"
                .formatted(
                        parameter.getName(),
                        parameter.getDeclaringExecutable().getName(),
                        RmlParam.class.getSimpleName(),
                        FnoParam.class.getSimpleName()));
    }
}
