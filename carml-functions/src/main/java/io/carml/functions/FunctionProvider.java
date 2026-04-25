package io.carml.functions;

import java.util.Collection;

/** SPI for supplying {@link FunctionDescriptor} instances to a {@link FunctionRegistry}. */
public interface FunctionProvider {

    /** Returns all function descriptors provided by this provider. */
    Collection<FunctionDescriptor> getFunctions();
}
