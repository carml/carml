package io.carml.output;

import java.io.Serial;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import org.eclipse.rdf4j.model.Value;

/**
 * Access-ordered LRU cache mapping {@link Value} instances to their pre-encoded UTF-8 byte
 * representations. Uses {@link LinkedHashMap} with access-ordering and
 * {@link LinkedHashMap#removeEldestEntry} override for bounded eviction.
 *
 * <p>Package-private: shared between {@link AbstractFastRdfSerializer} and
 * {@link NTriplesTermEncoder}.
 */
@SuppressWarnings("java:S2160") // equals/hashCode not needed -- internal cache, never compared
final class LruTermCache extends LinkedHashMap<Value, byte[]> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final int maxSize;

    LruTermCache(int maxSize) {
        super(Math.min(maxSize, 16), 0.75f, true);
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Value, byte[]> eldest) {
        return size() > maxSize;
    }

    /**
     * Returns the cached byte encoding for the given term, computing and caching it via the
     * supplied encoder function on first encounter.
     *
     * <p>Uses explicit {@code get}/{@code put} instead of {@code computeIfAbsent} because
     * {@code computeIfAbsent} is not safe on access-ordered {@link LinkedHashMap} — the structural
     * modification triggered by access-order reordering during the compute violates the
     * {@code computeIfAbsent} contract and can cause an {@link IllegalStateException} in some JDK
     * versions.
     *
     * @param term the RDF value to look up or encode
     * @param encoder the function to compute the byte encoding if not yet cached
     * @return the byte encoding for the term
     */
    byte[] getOrCompute(Value term, Function<Value, byte[]> encoder) {
        var cached = get(term);
        if (cached != null) {
            return cached;
        }
        var computed = encoder.apply(term);
        put(term, computed);
        return computed;
    }
}
