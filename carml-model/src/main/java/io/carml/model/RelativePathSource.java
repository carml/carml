package io.carml.model;

public interface RelativePathSource extends Source {

    Object getRoot();

    String getPath();
}
