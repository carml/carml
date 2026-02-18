package io.carml.model;

import org.eclipse.rdf4j.model.Value;

public interface FilePath extends Source {

    Value getRoot();

    String getPath();
}
