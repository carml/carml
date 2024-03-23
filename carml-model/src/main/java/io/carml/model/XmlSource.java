package io.carml.model;

import java.util.Set;

public interface XmlSource extends Source {

    Set<Namespace> getDeclaredNamespaces();
}
