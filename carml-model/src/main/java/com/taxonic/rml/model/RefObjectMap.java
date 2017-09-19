package com.taxonic.rml.model;

import java.util.Set;

public interface RefObjectMap extends BaseObjectMap {
	
	  TriplesMap getParentTriplesMap();

	  Set<Join> getJoinConditions();

}