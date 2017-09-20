package com.taxonic.carml.model;

import java.util.Set;

public interface TriplesMap {

	LogicalSource getLogicalSource();
	
	SubjectMap getSubjectMap();
	
	Set<PredicateObjectMap> getPredicateObjectMaps();

}
