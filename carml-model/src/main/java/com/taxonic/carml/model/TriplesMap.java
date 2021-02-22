package com.taxonic.carml.model;

import java.util.Set;

public interface TriplesMap extends Resource {

	LogicalSource getLogicalSource();

	SubjectMap getSubjectMap();

	Set<PredicateObjectMap> getPredicateObjectMaps();

	Set<NestedMapping> getNestedMappings();

}
