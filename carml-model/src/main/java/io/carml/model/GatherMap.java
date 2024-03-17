package io.carml.model;

import java.util.List;
import org.eclipse.rdf4j.model.IRI;

public interface GatherMap extends TermMap {

    Strategy getStrategy();

    IRI getGatherAs();

    List<ObjectMap> getGatheredOnes();

    boolean getAllowEmptyListAndContainer();
}
