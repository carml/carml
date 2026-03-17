package io.carml.engine.rdf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.datatype.XMLGregorianCalendar;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * A {@link ValueFactory} decorator that caches {@link IRI} instances in a bounded
 * {@link ConcurrentHashMap}. Wraps any delegate {@link ValueFactory}, adding IRI interning while
 * preserving the delegate's behavior for all other value types (BNodes, Literals, Statements).
 *
 * <p>For mappings with repeated predicate IRIs, class IRIs, and datatype IRIs, this eliminates
 * duplicate object allocations. Composable with any {@link ValueFactory}:
 * <pre>{@code
 * // Interning + simple (default)
 * new InterningValueFactory(SimpleValueFactory.getInstance())
 *
 * // Interning + validation
 * new InterningValueFactory(new ValidatingValueFactory())
 *
 * // Interning + custom
 * new InterningValueFactory(myCustomFactory)
 * }</pre>
 *
 * <p>BNodes are <b>not</b> cached because each blank node must have a unique identity. Literals
 * are <b>not</b> cached because their values are row-specific. However, literal datatype IRIs
 * benefit from the cache because {@code createLiteral(String, IRI)} receives an already-created
 * IRI instance.
 *
 * <p>Thread safety is provided by {@link ConcurrentHashMap}. The cache is bounded: once
 * {@code maxCacheSize} is reached, new IRI strings that are not already cached will produce fresh
 * allocations (existing cached entries are still returned). Under concurrent access, the cache may
 * slightly exceed {@code maxCacheSize} (by at most the number of concurrent threads) due to the
 * non-atomic size check — this is acceptable for a soft bound.
 *
 * <p><b>Note:</b> The {@link #createIRI(String, String)} override concatenates namespace and
 * localName before caching, so cached IRIs do not preserve the original namespace/localName split.
 * This is acceptable because CARML's engine does not call {@code getNamespace()}/{@code
 * getLocalName()} on generated IRIs.
 */
public class InterningValueFactory implements ValueFactory {

    /** Default maximum number of IRI entries in the cache. */
    public static final int DEFAULT_MAX_CACHE_SIZE = 65_536;

    private final ValueFactory delegate;

    private final ConcurrentHashMap<String, IRI> iriCache;

    private final int maxCacheSize;

    /**
     * Creates an interning value factory wrapping the given delegate with the specified cache size.
     *
     * @param delegate the value factory to delegate non-IRI creation to
     * @param maxCacheSize the maximum number of IRI entries to cache; must be positive
     */
    public InterningValueFactory(ValueFactory delegate, int maxCacheSize) {
        if (maxCacheSize <= 0) {
            throw new IllegalArgumentException("maxCacheSize must be positive, but was: %s".formatted(maxCacheSize));
        }
        this.delegate = delegate;
        this.maxCacheSize = maxCacheSize;
        this.iriCache = new ConcurrentHashMap<>();
    }

    /**
     * Creates an interning value factory wrapping the given delegate with the
     * {@link #DEFAULT_MAX_CACHE_SIZE default} cache size.
     */
    public InterningValueFactory(ValueFactory delegate) {
        this(delegate, DEFAULT_MAX_CACHE_SIZE);
    }

    /** Creates an interning value factory wrapping {@link SimpleValueFactory} with the default cache size. */
    public InterningValueFactory() {
        this(SimpleValueFactory.getInstance());
    }

    @Override
    public IRI createIRI(String iri) {
        if (iriCache.size() < maxCacheSize) {
            return iriCache.computeIfAbsent(iri, delegate::createIRI);
        }
        IRI cached = iriCache.get(iri);
        return cached != null ? cached : delegate.createIRI(iri);
    }

    @Override
    public IRI createIRI(String namespace, String localName) {
        return createIRI(namespace + localName);
    }

    /**
     * Returns the current number of cached IRI entries.
     *
     * @return the cache size
     */
    public int cacheSize() {
        return iriCache.size();
    }

    // --- Delegate all non-IRI methods ---

    @Override
    public BNode createBNode() {
        return delegate.createBNode();
    }

    @Override
    public BNode createBNode(String nodeID) {
        return delegate.createBNode(nodeID);
    }

    @Override
    public Literal createLiteral(String label) {
        return delegate.createLiteral(label);
    }

    @Override
    public Literal createLiteral(String label, String language) {
        return delegate.createLiteral(label, language);
    }

    @Override
    public Literal createLiteral(String label, IRI datatype) {
        return delegate.createLiteral(label, datatype);
    }

    @Override
    public Literal createLiteral(String label, CoreDatatype datatype) {
        return delegate.createLiteral(label, datatype);
    }

    @Override
    public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
        return delegate.createLiteral(label, datatype, coreDatatype);
    }

    @Override
    public Literal createLiteral(boolean value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(byte value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(short value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(int value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(long value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(float value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(double value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(BigDecimal value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(BigInteger value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(TemporalAccessor value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(TemporalAmount value) {
        return delegate.createLiteral(value);
    }

    @Override
    public Literal createLiteral(XMLGregorianCalendar calendar) {
        return delegate.createLiteral(calendar);
    }

    @Override
    public Literal createLiteral(Date date) {
        return delegate.createLiteral(date);
    }

    @Override
    public Statement createStatement(Resource subject, IRI predicate, Value object) {
        return delegate.createStatement(subject, predicate, object);
    }

    @Override
    public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
        return delegate.createStatement(subject, predicate, object, context);
    }

    @Override
    public Triple createTriple(Resource subject, IRI predicate, Value object) {
        return delegate.createTriple(subject, predicate, object);
    }
}
