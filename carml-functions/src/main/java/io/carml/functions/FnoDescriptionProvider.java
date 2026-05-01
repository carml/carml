package io.carml.functions;

import io.carml.vocab.Rdf.Fno;
import io.carml.vocab.Rdf.Fnoi;
import io.carml.vocab.Rdf.Fnom;
import java.io.Serial;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.RDFCollections;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;

/**
 * A {@link FunctionProvider} that loads function descriptions and Java implementation bindings
 * from an RDF model using the standard FnO (Function Ontology) vocabulary.
 *
 * <p>Descriptor creation is driven by {@code fno:Mapping} resources. For each mapping, the
 * provider resolves the bound Java class and method, then assembles parameter and return
 * descriptors from any of the following sources (highest priority first):
 *
 * <ol>
 *   <li>{@code fnom:PositionParameterMapping} entries that explicitly bind function parameters to
 *       Java argument positions, and {@code fnom:DefaultReturnMapping} entries that bind the
 *       method's return value to one or more {@code fno:Output} resources.
 *   <li>The bound function's {@code fno:Function} declaration ({@code fno:expects} /
 *       {@code fno:returns}). When parameter or output resources carry an {@code fno:predicate},
 *       both the resource IRI and the predicate IRI become valid match keys.
 *   <li>Reflection on the resolved Java {@link Method}, used as a fallback when no
 *       {@code fno:Function} declaration is present in the model.
 * </ol>
 *
 * <p>Static methods are supported: when the resolved method is {@code static}, no instance is
 * created and the method is invoked on a {@code null} target.
 */
@Slf4j
public class FnoDescriptionProvider implements FunctionProvider {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

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
        var result = new ArrayList<FunctionDescriptor>();

        model.filter(null, RDF.TYPE, Fno.Mapping).subjects().forEach(mappingResource -> {
            try {
                parseMapping(model, mappingResource).ifPresent(result::add);
            } catch (FnoDescriptionException exception) {
                LOG.warn("Skipping fno:Mapping '{}': {}", mappingResource, exception.getMessage());
            } catch (Exception exception) {
                LOG.warn("Skipping fno:Mapping '{}': failed to create descriptor", mappingResource, exception);
            }
        });

        return List.copyOf(result);
    }

    /**
     * Build a descriptor from a single {@code fno:Mapping}. When the bound function has no
     * {@code fno:Function} declaration the descriptor is derived purely from the resolved Java
     * method: parameters get synthetic IRIs ({@code <functionIri>#param-N}) that no real RML-FNML
     * execution can reference, so the standalone "no fno:Function" branch only produces correct
     * runtime behavior when paired with a {@code fnom:PositionParameterMapping} that supplies
     * real IRIs (or when the function takes zero arguments).
     */
    private static Optional<FunctionDescriptor> parseMapping(Model model, Resource mappingResource) {
        var binding = parseMappingBinding(model, mappingResource);

        var clazz = loadClass(binding.className(), binding.functionIri());

        // hasFunctionDecl distinguishes "fno:Function declared with zero parameters" from
        // "no fno:Function declared at all": the former fixes the method arity to 0 (helps
        // disambiguate overloads), the latter falls back to method reflection.
        var hasFunctionDecl = model.contains(binding.functionIri(), RDF.TYPE, Fno.Function);
        var declaredParameters = hasFunctionDecl
                ? parseFunctionParameters(model, binding.functionIri())
                : List.<ParameterDescriptor>of();
        var declaredReturns =
                hasFunctionDecl ? parseFunctionReturns(model, binding.functionIri()) : List.<ReturnDescriptor>of();

        Integer paramCountFromFunction = hasFunctionDecl ? declaredParameters.size() : null;
        var method = findMethodForBinding(clazz, binding, paramCountFromFunction);

        var parameters =
                hasFunctionDecl ? declaredParameters : deriveParametersFromMethod(binding.functionIri(), method);
        var returns = resolveReturns(model, hasFunctionDecl, declaredReturns, binding.returnOutputs(), method);

        var argSlots = buildArgSlots(parameters, binding.parameterPositions(), method, binding.functionIri());

        var target = Modifier.isStatic(method.getModifiers()) ? null : instantiate(clazz, binding.functionIri());

        return Optional.of(
                new ReflectiveFunctionDescriptor(binding.functionIri(), parameters, returns, target, method, argSlots));
    }

    private static MappingBinding parseMappingBinding(Model model, Resource mappingResource) {
        var functionIri = getObjectIri(model, mappingResource, Fno.function)
                .orElseThrow(() -> new FnoDescriptionException(
                        "fno:function missing on fno:Mapping '%s'".formatted(mappingResource)));

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

        var parameterPositions = parsePositionParameterMappings(model, mappingResource);
        var returnOutputs = parseDefaultReturnMappings(model, mappingResource);

        return new MappingBinding(functionIri, className, methodName, parameterPositions, returnOutputs);
    }

    private static Map<IRI, Integer> parsePositionParameterMappings(Model model, Resource mappingResource) {
        var positions = model.filter(mappingResource, Fno.parameterMapping, null).objects().stream()
                .filter(Resource.class::isInstance)
                .map(Resource.class::cast)
                .filter(pm -> model.contains(pm, RDF.TYPE, Fnom.PositionParameterMapping))
                .collect(Collectors.toMap(
                        pm -> getObjectIri(model, pm, Fnom.functionParameter)
                                .orElseThrow(() -> new FnoDescriptionException(
                                        "fnom:functionParameter missing on parameter mapping '%s'".formatted(pm))),
                        pm -> parsePositionLiteral(model, pm),
                        (a, b) -> {
                            throw new FnoDescriptionException(
                                    "fnom:functionParameter referenced by more than one fnom:PositionParameterMapping on fno:Mapping '%s'"
                                            .formatted(mappingResource));
                        },
                        LinkedHashMap::new));
        validatePositionRange(positions, mappingResource);
        return Map.copyOf(positions);
    }

    /**
     * Verify that the position values form a contiguous {@code [0, N)} range with no duplicates.
     * The duplicate check in the toMap merge function only catches duplicate parameter IRIs;
     * this catches duplicate <em>positions</em> assigned to distinct parameters and any sparse
     * gaps that would leave a method argument slot unfilled.
     */
    private static void validatePositionRange(Map<IRI, Integer> positions, Resource mappingResource) {
        if (positions.isEmpty()) {
            return;
        }
        int n = positions.size();
        var seen = new boolean[n];
        for (var entry : positions.entrySet()) {
            int p = entry.getValue();
            if (p < 0 || p >= n) {
                throw new FnoDescriptionException(
                        "fnom:implementationParameterPosition %d on fno:Mapping '%s' is out of the contiguous [0,%d) range expected for %d position mapping(s)"
                                .formatted(p, mappingResource, n, n));
            }
            if (seen[p]) {
                throw new FnoDescriptionException(
                        "fnom:implementationParameterPosition %d assigned to more than one parameter on fno:Mapping '%s'"
                                .formatted(p, mappingResource));
            }
            seen[p] = true;
        }
    }

    private static int parsePositionLiteral(Model model, Resource positionMapping) {
        var positionLiteral = getObjectLiteral(model, positionMapping, Fnom.implementationParameterPosition)
                .orElseThrow(() -> new FnoDescriptionException(
                        "fnom:implementationParameterPosition missing on parameter mapping '%s'"
                                .formatted(positionMapping)));
        try {
            return Integer.parseInt(positionLiteral.getLabel());
        } catch (NumberFormatException exception) {
            throw new FnoDescriptionException("fnom:implementationParameterPosition '%s' is not an integer on '%s'"
                    .formatted(positionLiteral.getLabel(), positionMapping));
        }
    }

    private static List<IRI> parseDefaultReturnMappings(Model model, Resource mappingResource) {
        return model.filter(mappingResource, Fno.returnMapping, null).objects().stream()
                .filter(Resource.class::isInstance)
                .map(Resource.class::cast)
                .filter(rm -> model.contains(rm, RDF.TYPE, Fnom.DefaultReturnMapping))
                .map(rm -> getObjectIri(model, rm, Fnom.functionOutput).orElse(null))
                .filter(Objects::nonNull)
                .distinct()
                .toList();
    }

    /**
     * Returns the parameter list declared via {@code fno:expects} on the given function. The
     * caller is responsible for first verifying that an {@code fno:Function} declaration exists
     * (an empty list here means the function declares zero parameters, not "no declaration").
     */
    private static List<ParameterDescriptor> parseFunctionParameters(Model model, IRI functionIri) {
        var expectsValues = model.filter(functionIri, Fno.expects, null).objects();
        return expectsValues.stream()
                .filter(Resource.class::isInstance)
                .map(Resource.class::cast)
                .findFirst()
                .map(listHead -> {
                    var paramResources = new ArrayList<Value>();
                    RDFCollections.asValues(model, listHead, paramResources);
                    return paramResources.stream()
                            .filter(Resource.class::isInstance)
                            .map(Resource.class::cast)
                            .map(paramResource -> parseParameter(model, paramResource))
                            .toList();
                })
                .orElse(List.of());
    }

    private static ParameterDescriptor parseParameter(Model model, Resource paramResource) {
        if (!(paramResource instanceof IRI parameterIri)) {
            throw new FnoDescriptionException(
                    "fno:Parameter must be an IRI resource, got blank node '%s'".formatted(paramResource));
        }

        var predicateIri = getObjectIri(model, paramResource, Fno.predicate).orElse(null);

        var javaType = getObjectIri(model, paramResource, Fno.type)
                .<Class<?>>map(FnoDescriptionProvider::mapXsdType)
                .orElse(Object.class);

        var required = getObjectLiteral(model, paramResource, Fno.required)
                .map(lit -> Boolean.parseBoolean(lit.getLabel()))
                .orElse(true);

        return new ParameterDescriptor(parameterIri, predicateIri, javaType, required);
    }

    /**
     * Returns the return list declared via {@code fno:returns} on the given function. When a
     * declaration exists but has no {@code fno:returns} the result is a single anonymous
     * {@code Object} return. The caller is responsible for first verifying that an
     * {@code fno:Function} declaration exists.
     */
    private static List<ReturnDescriptor> parseFunctionReturns(Model model, IRI functionIri) {
        var returnsValues = model.filter(functionIri, Fno.returns, null).objects();
        return returnsValues.stream()
                .filter(Resource.class::isInstance)
                .map(Resource.class::cast)
                .findFirst()
                .map(listHead -> {
                    var outputResources = new ArrayList<Value>();
                    RDFCollections.asValues(model, listHead, outputResources);
                    return outputResources.stream()
                            .filter(Resource.class::isInstance)
                            .map(Resource.class::cast)
                            .map(outputResource -> parseReturn(model, outputResource))
                            .toList();
                })
                .orElseGet(() -> List.of(new ReturnDescriptor(null, Object.class)));
    }

    private static ReturnDescriptor parseReturn(Model model, Resource outputResource) {
        var outputIri = outputResource instanceof IRI iri ? iri : null;

        var predicateIri = getObjectIri(model, outputResource, Fno.predicate).orElse(null);

        var javaType = getObjectIri(model, outputResource, Fno.type)
                .<Class<?>>map(FnoDescriptionProvider::mapXsdType)
                .orElse(Object.class);

        return new ReturnDescriptor(outputIri, predicateIri, javaType);
    }

    private static List<ParameterDescriptor> deriveParametersFromMethod(IRI functionIri, Method method) {
        var paramTypes = method.getParameterTypes();
        var result = new ArrayList<ParameterDescriptor>(paramTypes.length);
        for (int i = 0; i < paramTypes.length; i++) {
            IRI syntheticIri = VF.createIRI(functionIri.stringValue() + "#param-" + i);
            result.add(new ParameterDescriptor(syntheticIri, null, paramTypes[i], true));
        }
        return List.copyOf(result);
    }

    private static List<ReturnDescriptor> resolveReturns(
            Model model,
            boolean hasFunctionDecl,
            List<ReturnDescriptor> declaredReturns,
            List<IRI> mappingReturnOutputs,
            Method method) {
        if (!mappingReturnOutputs.isEmpty()) {
            return mappingReturnOutputs.stream()
                    .map(outputIri -> {
                        var predicateIri =
                                getObjectIri(model, outputIri, Fno.predicate).orElse(null);
                        var javaType = getObjectIri(model, outputIri, Fno.type)
                                .<Class<?>>map(FnoDescriptionProvider::mapXsdType)
                                .orElse(method.getReturnType());
                        return new ReturnDescriptor(outputIri, predicateIri, javaType);
                    })
                    .toList();
        }
        return hasFunctionDecl ? declaredReturns : List.of(new ReturnDescriptor(null, method.getReturnType()));
    }

    private static int[] buildArgSlots(
            List<ParameterDescriptor> parameters, Map<IRI, Integer> positions, Method method, IRI functionIri) {
        int paramCount = method.getParameterCount();
        if (positions.isEmpty()) {
            int[] slots = new int[paramCount];
            for (int i = 0; i < paramCount; i++) {
                slots[i] = i;
            }
            return slots;
        }

        if (parameters.size() != paramCount) {
            throw new FnoDescriptionException(
                    "Parameter count mismatch for function '%s': %d declared parameters but method takes %d args"
                            .formatted(functionIri, parameters.size(), paramCount));
        }

        int[] slots = new int[paramCount];
        var assigned = new boolean[paramCount];
        for (int i = 0; i < parameters.size(); i++) {
            var paramDesc = parameters.get(i);
            Integer position = positions.get(paramDesc.parameterIri());
            if (position == null && paramDesc.predicateIri() != null) {
                position = positions.get(paramDesc.predicateIri());
            }
            if (position == null) {
                throw new FnoDescriptionException(
                        "fnom:PositionParameterMapping missing for parameter '%s' on function '%s'"
                                .formatted(paramDesc.parameterIri(), functionIri));
            }
            if (position < 0 || position >= paramCount) {
                throw new FnoDescriptionException(
                        "fnom:implementationParameterPosition %d is out of range [0,%d) for function '%s'"
                                .formatted(position, paramCount, functionIri));
            }
            if (assigned[position]) {
                throw new FnoDescriptionException(
                        "fnom:implementationParameterPosition %d is assigned more than once for function '%s'"
                                .formatted(position, functionIri));
            }
            slots[position] = i;
            assigned[position] = true;
        }
        return slots;
    }

    private static Method findMethodForBinding(Class<?> clazz, MappingBinding binding, Integer paramCountFromFunction) {
        var candidates = Arrays.stream(clazz.getMethods())
                .filter(m -> m.getName().equals(binding.methodName()))
                .toList();
        if (candidates.isEmpty()) {
            throw new FnoDescriptionException("Method '%s' not found on class '%s' for function '%s'"
                    .formatted(binding.methodName(), clazz.getName(), binding.functionIri()));
        }

        var expectedCount = expectedParamCount(binding, paramCountFromFunction);
        if (expectedCount != null) {
            return candidates.stream()
                    .filter(m -> m.getParameterCount() == expectedCount)
                    .findFirst()
                    .orElseThrow(() -> new FnoDescriptionException(
                            "Method '%s' with %d parameter(s) not found on class '%s' for function '%s'"
                                    .formatted(
                                            binding.methodName(),
                                            expectedCount,
                                            clazz.getName(),
                                            binding.functionIri())));
        }

        if (candidates.size() > 1) {
            throw new FnoDescriptionException(
                    "Method '%s' is overloaded on class '%s' for function '%s'; declare an fno:Function with fno:expects, or use fnom:PositionParameterMapping to disambiguate"
                            .formatted(binding.methodName(), clazz.getName(), binding.functionIri()));
        }
        return candidates.get(0);
    }

    private static Integer expectedParamCount(MappingBinding binding, Integer paramCountFromFunction) {
        if (!binding.parameterPositions().isEmpty()) {
            var maxPosition = binding.parameterPositions().values().stream()
                    .mapToInt(Integer::intValue)
                    .max()
                    .orElse(-1);
            return Math.max(maxPosition + 1, binding.parameterPositions().size());
        }
        return paramCountFromFunction;
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

    private record MappingBinding(
            IRI functionIri,
            String className,
            String methodName,
            Map<IRI, Integer> parameterPositions,
            List<IRI> returnOutputs) {}

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
