package io.carml.engine;

public interface MergeableMappingResult<K, T> extends MappingResult<T> {

    K getKey();

    MergeableMappingResult<K, T> merge(MergeableMappingResult<K, T> other);
}
