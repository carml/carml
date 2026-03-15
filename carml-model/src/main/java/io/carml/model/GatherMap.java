package io.carml.model;

import java.util.List;
import org.eclipse.rdf4j.model.IRI;

/**
 * Represents an RML GatherMap, which collects terms into an RDF collection or container.
 *
 * <p>The {@link #getGathers()} method returns a list of {@link BaseObjectMap} items, which can be
 * either {@link ObjectMap} (term maps with reference/template/constant expressions) or
 * {@link RefObjectMap} (referencing object maps with a parentTriplesMap and optional join conditions).
 */
public interface GatherMap extends TermMap {

    IRI getStrategy();

    IRI getGatherAs();

    List<BaseObjectMap> getGathers();

    boolean getAllowEmptyListAndContainer();

    SubjectMap asSubjectMap();
}
