package io.carml.model;

import java.util.List;
import org.eclipse.rdf4j.model.IRI;

public interface GatherMap extends TermMap {

    IRI getStrategy();

    IRI getGatherAs();

    List<ObjectMap> getGathers();

    boolean getAllowEmptyListAndContainer();

    SubjectMap asSubjectMap();
}
