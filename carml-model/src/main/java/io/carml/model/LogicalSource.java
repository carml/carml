package io.carml.model;

import java.util.Set;
import org.eclipse.rdf4j.model.IRI;

public interface LogicalSource extends Resource {

    Source getSource();

    String getIterator();

    IRI getReferenceFormulation();

    String getTableName();

    String getSqlQuery();

    IRI getSqlVersion();

    String getQuery();

    Set<String> getExpressions();

    void setExpressions(Set<String> expressions);
}
