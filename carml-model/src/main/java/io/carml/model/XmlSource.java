package io.carml.model;

import java.util.Set;

public interface XmlSource extends Resource {

    Set<Namespace> getDeclaredNamespaces();
}
