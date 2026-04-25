package io.carml.functions;

import io.carml.vocab.Rdf.Fno;
import io.carml.vocab.Rdf.Fnoi;
import io.carml.vocab.Rdf.Fnom;
import java.io.Serial;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.RDFCollections;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;

/**
 * A {@link FunctionProvider} that loads function descriptions and Java implementation bindings from
 * an RDF model using the standard FnO (Function Ontology) vocabulary.
 *
 * <p>The RDF model is expected to contain two kinds of descriptions:
 *
 * <ol>
 *   <li><strong>Function descriptions</strong> ({@code fno:Function}) with parameter and return
 *       specifications via {@code fno:expects} and {@code fno:returns} RDF lists.
 *   <li><strong>Implementation mappings</strong> ({@code fno:Mapping}) linking functions to Java
 *       classes and methods via the {@code fnoi:} and {@code fnom:} vocabularies.
 * </ol>
 *
 * <p>Parameter binding is positional: the order of elements in the {@code fno:expects} RDF list
 * determines the order of Java method parameters.
 */
@Slf4j
public class FnoDescriptionProvider implements FunctionProvider {

    private final List<FunctionDescriptor> descriptors;

    /**
     * Creates a new provider by parsing function descriptions and implementation bindings from the
     * given RDF model.
     *
     * @param fnoModel the RDF model containing FnO function descriptions and mappings
     */
    public FnoDescriptionProvider(Model fnoModel) {
        if (fnoModel == null) {
            throw new IllegalArgumentException("fnoModel must not be null");
        }
        this.descriptors = parseFunctions(fnoModel);
    }

    @Override
    public Collection<FunctionDescriptor> getFunctions() {
        return descriptors;
    }

    private static List<FunctionDescriptor> parseFunctions(Model model) {
        var mappingBindings = parseMappingBindings(model);

        var result = new ArrayList<FunctionDescriptor>();

        model.filter(null, RDF.TYPE, Fno.Function).subjects().forEach(functionResource -> {
            if (!(functionResource instanceof IRI functionIri)) {
                return;
            }

            var binding = mappingBindings.stream()
                    .filter(b -> b.functionIri().equals(functionIri))
                    .findFirst();

            if (binding.isEmpty()) {
                LOG.debug("No fno:Mapping found for function '{}'; skipping.", functionIri);
                return;
            }

            try {
                var descriptor = createDescriptor(model, functionIri, binding.get());
                result.add(descriptor);
            } catch (FnoDescriptionException exception) {
                throw exception;
            } catch (Exception exception) {
                throw new FnoDescriptionException(
                        "Failed to create descriptor for function '%s'".formatted(functionIri), exception);
            }
        });

        return List.copyOf(result);
    }

    private static FunctionDescriptor createDescriptor(Model model, IRI functionIri, MappingBinding binding) {
        var parameters = parseParameters(model, functionIri);
        var returns = parseReturns(model, functionIri);
        var paramIris =
                parameters.stream().map(ParameterDescriptor::parameterIri).toList();

        var clazz = loadClass(binding.className(), functionIri);
        var target = instantiate(clazz, functionIri);
        var method = findMethod(clazz, binding.methodName(), paramIris.size(), functionIri);

        return new ReflectiveFunctionDescriptor(functionIri, parameters, returns, target, method, paramIris);
    }

    private static List<ParameterDescriptor> parseParameters(Model model, IRI functionIri) {
        var expectsValues = model.filter(functionIri, Fno.expects, null).objects();

        for (Value listHead : expectsValues) {
            if (listHead instanceof Resource listResource) {
                var paramResources = new ArrayList<Value>();
                RDFCollections.asValues(model, listResource, paramResources);

                return paramResources.stream()
                        .filter(Resource.class::isInstance)
                        .map(Resource.class::cast)
                        .map(paramResource -> parseParameter(model, paramResource))
                        .toList();
            }
        }

        return List.of();
    }

    private static ParameterDescriptor parseParameter(Model model, Resource paramResource) {
        if (!(paramResource instanceof IRI parameterIri)) {
            throw new FnoDescriptionException(
                    "fno:Parameter must be an IRI resource, got blank node '%s'".formatted(paramResource));
        }

        var javaType = getObjectIri(model, paramResource, Fno.type)
                .<Class<?>>map(FnoDescriptionProvider::mapXsdType)
                .orElse(Object.class);

        var required = getObjectLiteral(model, paramResource, Fno.required)
                .map(lit -> Boolean.parseBoolean(lit.getLabel()))
                .orElse(true);

        return new ParameterDescriptor(parameterIri, javaType, required);
    }

    private static List<ReturnDescriptor> parseReturns(Model model, IRI functionIri) {
        var returnsValues = model.filter(functionIri, Fno.returns, null).objects();

        for (Value listHead : returnsValues) {
            if (listHead instanceof Resource listResource) {
                var outputResources = new ArrayList<Value>();
                RDFCollections.asValues(model, listResource, outputResources);

                return outputResources.stream()
                        .filter(Resource.class::isInstance)
                        .map(Resource.class::cast)
                        .map(outputResource -> parseReturn(model, outputResource))
                        .toList();
            }
        }

        return List.of(new ReturnDescriptor(null, Object.class));
    }

    private static ReturnDescriptor parseReturn(Model model, Resource outputResource) {
        var outputIri = outputResource instanceof IRI iri ? iri : null;

        var javaType = getObjectIri(model, outputResource, Fno.type)
                .<Class<?>>map(FnoDescriptionProvider::mapXsdType)
                .orElse(Object.class);

        return new ReturnDescriptor(outputIri, javaType);
    }

    private static List<MappingBinding> parseMappingBindings(Model model) {
        var result = new ArrayList<MappingBinding>();

        model.filter(null, RDF.TYPE, Fno.Mapping).subjects().forEach(mappingResource -> {
            var functionIri = getRequiredObjectIri(
                    model,
                    mappingResource,
                    Fno.function,
                    "fno:function missing on fno:Mapping '%s'".formatted(mappingResource));

            var implResource = getRequiredObjectResource(
                    model,
                    mappingResource,
                    Fno.implementation,
                    "fno:implementation missing on fno:Mapping '%s'".formatted(mappingResource));

            var className = getRequiredObjectLiteral(
                            model,
                            implResource,
                            Fnoi.class_name,
                            "fnoi:class-name missing on implementation '%s'".formatted(implResource))
                    .getLabel();

            var methodMappingResource = getRequiredObjectResource(
                    model,
                    mappingResource,
                    Fno.methodMapping,
                    "fno:methodMapping missing on fno:Mapping '%s'".formatted(mappingResource));

            var methodName = getRequiredObjectLiteral(
                            model,
                            methodMappingResource,
                            Fnom.method_name,
                            "fnom:method-name missing on method mapping '%s'".formatted(methodMappingResource))
                    .getLabel();

            result.add(new MappingBinding(functionIri, className, methodName));
        });

        return List.copyOf(result);
    }

    private static Class<?> loadClass(String className, IRI functionIri) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException exception) {
            throw new FnoDescriptionException(
                    "Class '%s' not found for function '%s'".formatted(className, functionIri), exception);
        }
    }

    private static Object instantiate(Class<?> clazz, IRI functionIri) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException exception) {
            throw new FnoDescriptionException(
                    "Failed to instantiate class '%s' for function '%s'".formatted(clazz.getName(), functionIri),
                    exception);
        }
    }

    private static Method findMethod(Class<?> clazz, String methodName, int paramCount, IRI functionIri) {
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals(methodName) && method.getParameterCount() == paramCount) {
                return method;
            }
        }
        throw new FnoDescriptionException("Method '%s' with %d parameter(s) not found on class '%s' for function '%s'"
                .formatted(methodName, paramCount, clazz.getName(), functionIri));
    }

    static Class<?> mapXsdType(IRI typeIri) {
        if (XSD.STRING.equals(typeIri)) {
            return String.class;
        }
        if (XSD.INTEGER.equals(typeIri) || XSD.INT.equals(typeIri)) {
            return Integer.class;
        }
        if (XSD.LONG.equals(typeIri)) {
            return Long.class;
        }
        if (XSD.DOUBLE.equals(typeIri)) {
            return Double.class;
        }
        if (XSD.FLOAT.equals(typeIri)) {
            return Float.class;
        }
        if (XSD.BOOLEAN.equals(typeIri)) {
            return Boolean.class;
        }
        if (XSD.DECIMAL.equals(typeIri)) {
            return Double.class;
        }
        if (RDF.LIST.equals(typeIri)) {
            return List.class;
        }
        return Object.class;
    }

    // -- RDF model helper methods --

    private static IRI getRequiredObjectIri(Model model, Resource subject, IRI predicate, String errorMessage) {
        return getObjectIri(model, subject, predicate).orElseThrow(() -> new FnoDescriptionException(errorMessage));
    }

    private static Optional<IRI> getObjectIri(Model model, Resource subject, IRI predicate) {
        return model.filter(subject, predicate, null).objects().stream()
                .filter(IRI.class::isInstance)
                .map(IRI.class::cast)
                .findFirst();
    }

    private static Resource getRequiredObjectResource(
            Model model, Resource subject, IRI predicate, String errorMessage) {
        return model.filter(subject, predicate, null).objects().stream()
                .filter(Resource.class::isInstance)
                .map(Resource.class::cast)
                .findFirst()
                .orElseThrow(() -> new FnoDescriptionException(errorMessage));
    }

    private static Literal getRequiredObjectLiteral(Model model, Resource subject, IRI predicate, String errorMessage) {
        return getObjectLiteral(model, subject, predicate).orElseThrow(() -> new FnoDescriptionException(errorMessage));
    }

    private static Optional<Literal> getObjectLiteral(Model model, Resource subject, IRI predicate) {
        return model.filter(subject, predicate, null).objects().stream()
                .filter(Literal.class::isInstance)
                .map(Literal.class::cast)
                .findFirst();
    }

    private record MappingBinding(IRI functionIri, String className, String methodName) {}

    /** Exception thrown when FnO descriptions are invalid or cannot be resolved. */
    public static class FnoDescriptionException extends RuntimeException {

        @Serial
        private static final long serialVersionUID = 2470104354953140411L;

        public FnoDescriptionException(String message) {
            super(message);
        }

        public FnoDescriptionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
