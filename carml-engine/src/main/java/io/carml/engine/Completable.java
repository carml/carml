package io.carml.engine;

import java.util.stream.Stream;

public interface Completable<T> {

    Stream<T> complete();
}
