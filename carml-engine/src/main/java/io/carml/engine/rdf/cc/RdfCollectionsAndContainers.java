package io.carml.engine.rdf.cc;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.experimental.UtilityClass;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;

@UtilityClass
public class RdfCollectionsAndContainers {

    /**
     * Lazily emits the statements that describe an RDF container ({@code rdf:Bag}/{@code rdf:Seq}/
     * {@code rdf:Alt}). Produces {@code <container, rdf:type, containerType>} followed by
     * {@code <container, rdf:_<i>, value>} for {@code i = 1..N}. When {@code contexts} is empty a
     * single default-graph statement is emitted per slot; otherwise one statement per context is
     * emitted.
     *
     * @param containerType the container type IRI (e.g. {@code rdf:Bag})
     * @param values the member values, in order
     * @param container the container resource
     * @param valueFactory the value factory used to mint predicate IRIs and statements
     * @param contexts optional graph contexts; empty means default graph
     * @return a lazy stream of statements
     */
    public static Stream<Statement> toRdfContainerStatements(
            IRI containerType,
            List<Value> values,
            Resource container,
            ValueFactory valueFactory,
            Resource... contexts) {
        var typeStatements = createStatements(valueFactory, container, RDF.TYPE, containerType, contexts);

        var memberStatements = IntStream.range(0, values.size()).boxed().flatMap(index -> {
            var predicate = valueFactory.createIRI(RDF.NAMESPACE, "_" + (index + 1));
            return createStatements(valueFactory, container, predicate, values.get(index), contexts);
        });

        return Stream.concat(typeStatements, memberStatements);
    }

    /**
     * Lazily emits the statements that describe an RDF list (cons-cell encoding using
     * {@code rdf:first}/{@code rdf:rest}). Produces, for each element, a {@code <current,
     * rdf:first, value>} pair plus a {@code <current, rdf:rest, next>} pair, terminating with
     * {@code rdf:nil}. Fresh blank nodes for the spine are created via {@code valueFactory}.
     *
     * @param values the list elements, in order
     * @param head the resource that starts the list
     * @param valueFactory the value factory used to mint spine blank nodes and statements
     * @param contexts optional graph contexts; empty means default graph
     * @return a lazy stream of statements
     */
    public static Stream<Statement> toRdfListStatements(
            List<Value> values, Resource head, ValueFactory valueFactory, Resource... contexts) {
        if (values.isEmpty()) {
            return Stream.empty();
        }

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new ListStatementIterator(values, head, valueFactory, contexts), Spliterator.ORDERED),
                false);
    }

    private static Stream<Statement> createStatements(
            ValueFactory valueFactory, Resource subject, IRI predicate, Value object, Resource... contexts) {
        if (contexts == null || contexts.length == 0) {
            return Stream.of(valueFactory.createStatement(subject, predicate, object));
        }
        return Stream.of(contexts).map(context -> valueFactory.createStatement(subject, predicate, object, context));
    }

    /**
     * Iterator that emits the statements describing a single cons-cell at a time. State: the next
     * value index and the {@link Resource} for the current cell. On each call we emit the
     * {@code rdf:first} statement(s) for the current cell, then the {@code rdf:rest} statement(s)
     * (terminating with {@code rdf:nil} on the last element), and advance the spine pointer to a
     * fresh blank node.
     */
    private static final class ListStatementIterator implements Iterator<Statement> {

        private final List<Value> values;
        private final ValueFactory valueFactory;
        private final Resource[] contexts;

        // Statements for the current cell are buffered into a small internal queue (at most
        // contexts.length * 2 entries), then drained one-by-one before advancing the spine.
        private final ArrayDeque<Statement> pending = new ArrayDeque<>();

        private Resource currentSubject;
        private int valueIndex;

        ListStatementIterator(List<Value> values, Resource head, ValueFactory valueFactory, Resource[] contexts) {
            this.values = values;
            this.valueFactory = valueFactory;
            this.contexts = contexts;
            this.currentSubject = head;
            this.valueIndex = 0;
        }

        @Override
        public boolean hasNext() {
            ensurePending();
            return !pending.isEmpty();
        }

        @Override
        public Statement next() {
            ensurePending();
            if (pending.isEmpty()) {
                throw new NoSuchElementException();
            }
            return pending.poll();
        }

        private void ensurePending() {
            if (!pending.isEmpty() || valueIndex >= values.size()) {
                return;
            }
            var value = values.get(valueIndex);
            Consumer<Statement> sink = pending::add;
            createStatements(valueFactory, currentSubject, RDF.FIRST, value, contexts)
                    .forEach(sink);
            Resource rest = valueIndex == values.size() - 1 ? RDF.NIL : valueFactory.createBNode();
            createStatements(valueFactory, currentSubject, RDF.REST, rest, contexts)
                    .forEach(sink);
            currentSubject = rest;
            valueIndex++;
        }
    }
}
