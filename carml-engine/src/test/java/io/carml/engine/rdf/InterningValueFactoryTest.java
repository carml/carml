package io.carml.engine.rdf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

class InterningValueFactoryTest {

    @Test
    void createIRI_sameString_returnsCachedInstance() {
        var factory = new InterningValueFactory();
        IRI first = factory.createIRI("http://example.org/foo");
        IRI second = factory.createIRI("http://example.org/foo");

        assertThat(first, is(sameInstance(second)));
    }

    @Test
    void createIRI_differentStrings_returnsDifferentInstances() {
        var factory = new InterningValueFactory();
        IRI first = factory.createIRI("http://example.org/foo");
        IRI second = factory.createIRI("http://example.org/bar");

        assertThat(first, is(not(sameInstance(second))));
        assertThat(first.stringValue(), is("http://example.org/foo"));
        assertThat(second.stringValue(), is("http://example.org/bar"));
    }

    @Test
    void createIRI_namespaceLocalName_returnsCachedInstance() {
        var factory = new InterningValueFactory();
        IRI first = factory.createIRI("http://example.org/", "foo");
        IRI second = factory.createIRI("http://example.org/foo");

        assertThat(first, is(sameInstance(second)));
    }

    @Test
    void createIRI_cacheSize_reflectsUniqueEntries() {
        var factory = new InterningValueFactory();
        factory.createIRI("http://example.org/a");
        factory.createIRI("http://example.org/b");
        factory.createIRI("http://example.org/a"); // duplicate

        assertThat(factory.cacheSize(), is(2));
    }

    @Test
    void createIRI_cacheFull_returnsNewInstanceForUncachedIri() {
        var factory = new InterningValueFactory(SimpleValueFactory.getInstance(), 2);
        factory.createIRI("http://example.org/a");
        factory.createIRI("http://example.org/b");

        // Cache is now full (2 entries, max is 2)
        IRI third = factory.createIRI("http://example.org/c");
        IRI thirdAgain = factory.createIRI("http://example.org/c");

        // Third IRI is not cached, so each call produces a new instance
        assertThat(third, is(not(sameInstance(thirdAgain))));
        assertThat(third.stringValue(), is("http://example.org/c"));
        assertThat(factory.cacheSize(), is(2));
    }

    @Test
    void createIRI_cacheFull_stillReturnsCachedInstanceForExistingEntry() {
        var factory = new InterningValueFactory(SimpleValueFactory.getInstance(), 2);
        IRI first = factory.createIRI("http://example.org/a");
        factory.createIRI("http://example.org/b");

        // Cache is full, but "a" is already cached
        IRI firstAgain = factory.createIRI("http://example.org/a");
        assertThat(first, is(sameInstance(firstAgain)));
    }

    @Test
    void createBNode_noCaching_eachCallReturnsNewInstance() {
        var factory = new InterningValueFactory();
        var bnode1 = factory.createBNode("id1");
        var bnode2 = factory.createBNode("id1");

        // BNodes must not be cached — each call should return a new object
        assertThat(bnode1, is(not(sameInstance(bnode2))));
    }

    @Test
    void createBNode_noArgs_eachCallReturnsUniqueNode() {
        var factory = new InterningValueFactory();
        var bnode1 = factory.createBNode();
        var bnode2 = factory.createBNode();

        assertThat(bnode1.getID(), is(not(bnode2.getID())));
    }

    @Test
    void createLiteral_noCaching_eachCallReturnsNewInstance() {
        var factory = new InterningValueFactory();
        var lit1 = factory.createLiteral("hello");
        var lit2 = factory.createLiteral("hello");

        // Literals must not be cached
        assertThat(lit1, is(not(sameInstance(lit2))));
    }

    @Test
    void constructor_defaultCacheSize_usesDefault() {
        var factory = new InterningValueFactory();
        // Fill beyond default — just verify it works with many entries
        for (int i = 0; i < 100; i++) {
            factory.createIRI("http://example.org/" + i);
        }
        assertThat(factory.cacheSize(), is(100));
    }

    @Test
    void constructor_zeroCacheSize_throwsException() {
        var delegate = SimpleValueFactory.getInstance();
        assertThrows(IllegalArgumentException.class, () -> new InterningValueFactory(delegate, 0));
    }

    @Test
    void constructor_negativeCacheSize_throwsException() {
        var delegate = SimpleValueFactory.getInstance();
        assertThrows(IllegalArgumentException.class, () -> new InterningValueFactory(delegate, -1));
    }

    @Test
    void createIRI_withValidatingDelegate_delegatesValidation() {
        var validatingFactory = new org.eclipse.rdf4j.model.impl.ValidatingValueFactory();
        var factory = new InterningValueFactory(validatingFactory);

        // Valid IRI works
        var iri = factory.createIRI("http://example.org/valid");
        assertThat(iri.stringValue(), is("http://example.org/valid"));

        // Invalid IRI should throw (validation from delegate)
        assertThrows(Exception.class, () -> factory.createIRI("not a valid iri"));
    }

    @Test
    void createLiteral_withValidatingDelegate_delegatesValidation() {
        var validatingFactory = new org.eclipse.rdf4j.model.impl.ValidatingValueFactory();
        var factory = new InterningValueFactory(validatingFactory);

        // Literals are delegated to the validating factory
        var literal = factory.createLiteral("hello");
        assertThat(literal.getLabel(), is("hello"));
    }

    @Test
    void createIRI_cacheFull_namespacePlusLocalName_returnsNewInstance() {
        var factory = new InterningValueFactory(SimpleValueFactory.getInstance(), 1);
        // Fill cache with one entry
        factory.createIRI("http://example.org/first");

        // Two-arg overload when cache is full and IRI is not cached
        var iri1 = factory.createIRI("http://example.org/", "second");
        var iri2 = factory.createIRI("http://example.org/", "second");

        assertThat(iri1.stringValue(), is("http://example.org/second"));
        // Not cached — each call produces a new instance
        assertThat(iri1, is(not(sameInstance(iri2))));
    }
}
