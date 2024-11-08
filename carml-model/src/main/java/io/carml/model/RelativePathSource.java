package io.carml.model;

import org.eclipse.rdf4j.model.Value;

public interface RelativePathSource extends Source {

    Value getRoot();

    String getPath();
}
