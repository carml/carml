package io.carml.model;

import org.eclipse.rdf4j.model.IRI;

public interface LogicalSource extends AbstractLogicalSource {

    Source getSource();

    String getIterator();

    ReferenceFormulation getReferenceFormulation();

    String getTableName();

    String getSqlQuery();

    IRI getSqlVersion();

    String getQuery();
}
