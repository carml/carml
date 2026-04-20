package io.carml.engine.rdf.cc;

import java.io.Serial;
import java.util.Objects;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

/**
 * Composite merge key for graph-scoped mergeable collections.
 *
 * <p>Mergeables with the same head value but different graph contexts should not be merged
 * together, because each named graph requires its own independent set of blank nodes.
 *
 * <p>Implements {@link Value} so it can be used as the key type K in
 * {@link io.carml.engine.MergeableMappingResult MergeableMappingResult&lt;Value, Statement&gt;}.
 */
record GraphScopedMergeKey(Value head, Set<Resource> graphs) implements Value {

    @Serial
    private static final long serialVersionUID = 1L;

    GraphScopedMergeKey {
        Objects.requireNonNull(head);
        graphs = Set.copyOf(graphs);
    }

    /**
     * Extracts the actual head value from a merge key. If the key is a {@link GraphScopedMergeKey},
     * returns the wrapped head; otherwise returns the key itself.
     */
    static Value unwrap(Value key) {
        if (key instanceof GraphScopedMergeKey scopedKey) {
            return scopedKey.head();
        }
        return key;
    }

    @Override
    public String stringValue() {
        return head.stringValue() + "#graphs=" + graphs;
    }
}
