package io.carml.model;

public interface RelativePathSource extends Resource {

    Object getRoot();

    String getPath();
}
