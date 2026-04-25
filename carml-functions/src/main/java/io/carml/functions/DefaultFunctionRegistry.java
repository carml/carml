package io.carml.functions;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;

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
