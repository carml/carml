package com.taxonic.carml.model;

import java.util.Set;

public interface RefObjectMap extends BaseObjectMap {
	
	  TriplesMap getParentTriplesMap();

	  Set<Join> getJoinConditions();

}