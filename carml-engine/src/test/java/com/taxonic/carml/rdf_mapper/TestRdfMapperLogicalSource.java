 package com.taxonic.carml.rdf_mapper;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Iterables;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.XmlSource;

public class TestRdfMapperLogicalSource extends RmlLoader {
	
	@Test
	public void mapper_givenLogicalSourceSourceWithMultipleTypes_hasTraitsMatchingTypes() {
		Set<TriplesMap> mapping = loadRmlFromTtl("RdfMapper/simple.doublyTypedCarml.rml.ttl");
		
		assertThat(mapping.size(), is(1));
		
		TriplesMap tMap = Iterables.getOnlyElement(mapping);
		
		Object source = tMap.getLogicalSource().getSource();
		
		assertThat(source, instanceOf(NameableStream.class));
		assertThat(source, instanceOf(XmlSource.class));
	}

}
