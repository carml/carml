package com.taxonic.rml.model;

import java.util.Set;

public interface PredicateObjectMap {

	Set<PredicateMap> getPredicateMaps();
	
	Set<BaseObjectMap> getObjectMaps();
	
	Set<GraphMap> getGraphMaps();
	
}
