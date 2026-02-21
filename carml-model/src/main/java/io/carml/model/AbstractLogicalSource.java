package io.carml.model;

/**
 * Abstract base for both {@link LogicalSource} and {@code LogicalView}. TriplesMap.getLogicalSource()
 * returns this type to support both.
 */
public interface AbstractLogicalSource extends Resource {}
