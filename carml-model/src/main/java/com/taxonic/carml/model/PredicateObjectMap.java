package com.taxonic.carml.model;

import java.util.Set;

public interface PredicateObjectMap extends Resource {

	Set<PredicateMap> getPredicateMaps();

	Set<BaseObjectMap> getObjectMaps();

	Set<GraphMap> getGraphMaps();

}
