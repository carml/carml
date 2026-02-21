package io.carml.engine.function;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;

/** Mutable, thread-safe registry of {@link FunctionDescriptor} instances. */
public interface FunctionRegistry {

    /** Looks up a function descriptor by its IRI. */
    Optional<FunctionDescriptor> getFunction(IRI functionIri);

    /** Registers a function descriptor. Replaces any existing descriptor with the same IRI. */
    void register(FunctionDescriptor descriptor);

    /** Registers all function descriptors from the given provider. */
    void registerAll(FunctionProvider provider);

    /** Removes the function descriptor registered under the given IRI. */
    void unregister(IRI functionIri);

    /** Returns the IRIs of all currently registered functions. */
    Set<IRI> getRegisteredFunctions();

    /** Creates a new empty registry with the default in-memory implementation. */
    static FunctionRegistry create() {
        return new DefaultFunctionRegistry();
    }
}

class DefaultFunctionRegistry implements FunctionRegistry {

    private final ConcurrentHashMap<IRI, FunctionDescriptor> functions = new ConcurrentHashMap<>();

    @Override
    public Optional<FunctionDescriptor> getFunction(IRI functionIri) {
        return Optional.ofNullable(functions.get(functionIri));
    }

    @Override
    public void register(FunctionDescriptor descriptor) {
        if (descriptor == null) {
            throw new IllegalArgumentException("Function descriptor must not be null");
        }
        if (descriptor.getFunctionIri() == null) {
            throw new IllegalArgumentException("Function descriptor IRI must not be null");
        }
        functions.put(descriptor.getFunctionIri(), descriptor);
    }

    @Override
    public void registerAll(FunctionProvider provider) {
        provider.getFunctions().forEach(this::register);
    }

    @Override
    public void unregister(IRI functionIri) {
        functions.remove(functionIri);
    }

    @Override
    public Set<IRI> getRegisteredFunctions() {
        return functions.keySet().stream().collect(Collectors.toUnmodifiableSet());
    }
}
