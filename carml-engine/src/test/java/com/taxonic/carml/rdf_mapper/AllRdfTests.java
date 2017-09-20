package com.taxonic.carml.rdf_mapper;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestRdfMapper.class, TestRdfMapperBasic.class, TestRdfMapperGraphMaps.class,
		TestRdfMapperParentTriplesMap.class, TestRdfMapperTermType.class })
public class AllRdfTests {

}
