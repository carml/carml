package io.carml.functions;

import java.util.Optional;
import java.util.Set;
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
