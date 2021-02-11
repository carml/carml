package com.taxonic.carml.engine.iotests;

import org.junit.Test;

// TODO consider dynamically creating tests using junit and just iterating over sets of input files
public class RmlMapperTest extends MappingTest {

	@Test
	public void testTermTypeMappingBlankNodeC() {
		testMapping("RmlMapper",
				"RmlMapper/test14/termTypeMappingBlankNodeC.rml.ttl",
				"RmlMapper/test14/termTypeMappingBlankNodeC.output.ttl");
	}

	@Test
	public void testTermTypeMappingBlankNodeB() {
		testMapping("RmlMapper",
				"RmlMapper/test14/termTypeMappingBlankNodeB.rml.ttl",
				"RmlMapper/test14/termTypeMappingBlankNodeB.output.ttl");
	}

	@Test
	public void testTermTypeMappingBlankNodeA() {
		testMapping("RmlMapper",
				"RmlMapper/test14/termTypeMappingBlankNodeA.rml.ttl",
				"RmlMapper/test14/termTypeMappingBlankNodeA.output.ttl");
	}

	@Test
	public void testTermTypeMappingLiteral() {
		testMapping("RmlMapper",
				"RmlMapper/test14/termTypeMappingLiteral.rml.ttl",
				"RmlMapper/test14/termTypeMappingLiteral.output.ttl");
	}

	@Test
	public void testTermTypeMappingIRI() {
		testMapping("RmlMapper",
				"RmlMapper/test14/termTypeMappingIRI.rml.ttl",
				"RmlMapper/test14/termTypeMappingIRI.output.ttl");
	}

	@Test
	public void testGraphMapMappingMultipleGraphsB() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingMultipleGraphsB.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleGraphsB.output.trig");
	}

	@Test
	public void testGraphMapMappingMultipleGraphsA() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingMultipleGraphsA.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleGraphsA.output.trig");
	}

	@Test
	public void testGraphMapMappingMultipleClasses() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingMultipleClasses.rml.ttl",
				"RmlMapper/test13/graphMapMappingMultipleClasses.output.trig");
	}

	@Test
	public void testGraphMapMappingSubjectB() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingSubjectB.rml.ttl",
				"RmlMapper/test13/graphMapMappingSubjectB.output.trig");
	}

	@Test
	public void testGraphMapMappingPredObj() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingPredObj.rml.ttl",
				"RmlMapper/test13/graphMapMappingPredObj.output.trig");
	}

	@Test
	public void testGraphMapMappingSubjectA() {
		testMapping("RmlMapper",
				"RmlMapper/test13/graphMapMappingSubjectA.rml.ttl",
				"RmlMapper/test13/graphMapMappingSubjectA.output.trig");
	}

	@Test
	public void testRemoveNonLatinCharsFunction() {
		testMapping(
			"RmlMapper",
			"RmlMapper/test12/removeNonLatinCharsMapping.fnml.ttl",
			"RmlMapper/test12/removeNonLatinCharsMapping.output.ttl",
			m -> m.addFunctions(new RmlFunctions())
		);
	}

	@Test
	public void testToBoolFunction() {
		testMapping(
			"RmlMapper",
			"RmlMapper/test11/toBoolMapping.fnml.ttl",
			"RmlMapper/test11/toBoolMapping.output.ttl",
			m -> m.addFunctions(new RmlFunctions())
		);
	}

	@Test
	public void testNestedFunction() {
		testMapping(
			"RmlMapper",
			"RmlMapper/test18/nestedFunctionMapping.fnml.ttl",
			"RmlMapper/test18/nestedFunctionMapping.output.ttl",
			m -> m.addFunctions(new RmlFunctions())
		);
	}

	@Test
	public void testListReturningFunction() {
		testMapping(
			"RmlMapper",
			"RmlMapper/test18/listReturningFunctionMapping.rml.ttl",
			"RmlMapper/test18/listReturningFunctionMapping.output.ttl",
			m -> m.addFunctions(new RmlFunctions())
		);
	}

	@Test
	public void testListTakingFunction() {
		testMapping(
			"RmlMapper",
			"RmlMapper/test18/listTakingFunctionMapping.rml.ttl",
			"RmlMapper/test18/listTakingFunctionMapping.output.ttl",
			m -> m.addFunctions(new RmlFunctions())
		);
	}

	@Test
	public void testIriFunctionMapping() {
		testMapping(
			"RmlMapper",
			"RmlMapper/test18/iriFunctionMapping.rml.ttl",
			"RmlMapper/test18/iriFunctionMapping.output.ttl",
			m -> m.addFunctions(new RmlFunctions())
		);
	}

	@Test
	public void testIriFunctionMappingWithNulls() {
		testMapping(
			"RmlMapper",
			"RmlMapper/test18/iriFunctionMappingWithNulls.rml.ttl",
			"RmlMapper/test18/iriFunctionMappingWithNulls.output.ttl",
			m -> m.addFunctions(new RmlFunctions())
		);
	}

	@Test
	public void testJoinOnMultipleParentValues() {
		testMapping("RmlMapper",
			"RmlMapper/test19/joinOnMultipleParentValues.rml.ttl",
			"RmlMapper/test19/joinOnMultipleParentValues.output.ttl");
	}

	@Test
	public void testJoinPeopleOccupations() {
		testMapping("RmlMapper",
			"RmlMapper/test20/joinPeopleOccupations.rml.ttl",
			"RmlMapper/test20/joinPeopleOccupations.output.ttl");
	}

	@Test
	public void testOrdersNested() {
		testMapping("RmlMapper",
			"RmlMapper/test21/ordersNested.rml.ttl",
			"RmlMapper/test21/ordersNested.output.ttl");
	}

	@Test
	public void testOrdersNestedDefaultIterationAndEntryContext() {
		testMapping("RmlMapper",
			"RmlMapper/test22/ordersNested.rml.ttl",
			"RmlMapper/test22/ordersNested.output.ttl");
	}

	@Test
	public void testOrdersNestedSpecifiedIterationAndEntryContext() {
		testMapping("RmlMapper",
			"RmlMapper/test23/ordersNested.rml.ttl",
			"RmlMapper/test23/ordersNested.output.ttl");
	}

	@Test
	public void testOrdersObjectMapNested() {
		testMapping("RmlMapper",
			"RmlMapper/test24/ordersNested.rml.ttl",
			"RmlMapper/test24/ordersNested.output.ttl");
	}

	@Test
	public void testOrdersObjectMapNestedMergeSuper() {
		testMapping("RmlMapper",
			"RmlMapper/test25/ordersNested.rml.ttl",
			"RmlMapper/test25/ordersNested.output.ttl");
	}

	@Test
	public void testSeparateMapsMapping() {
		testMapping("RmlMapper",
				"RmlMapper/test10/separateMapsMapping.rml.ttl",
				"RmlMapper/test10/separateMapsMapping.output.ttl");
	}

	@Test
	public void testParentTriplesMapping() {
		testMapping("RmlMapper",
				"RmlMapper/test9/parentTriplesMapping.rml.ttl",
				"RmlMapper/test9/parentTriplesMapping.output.ttl");
	}

	@Test
	public void testSimpleBlankNodeMapping() {
		//TODO Note: algorithm for generating blank node ids will change
		testMapping("RmlMapper",
				"RmlMapper/test8/simpleBlankNodeMapping.rml.ttl",
				"RmlMapper/test8/simpleBlankNodeMapping.output.ttl");
	}

	@Test
	public void testSimple2ConstantMapping() {
		testMapping("RmlMapper",
				"RmlMapper/test7/simple2ConstantMapping.rml.ttl",
				"RmlMapper/test7/simple2ConstantMapping.output.ttl");
	}

	@Test
	public void testSimpleConstantMapping() {
		testMapping("RmlMapper",
				"RmlMapper/test6/simpleConstantMapping.rml.ttl",
				"RmlMapper/test6/simpleConstantMapping.output.ttl");
	}

	@Test
	public void testDataReferenceMapping() {
		testMapping("RmlMapper",
					"RmlMapper/test5/dataReferenceMapping.rml.ttl",
					"RmlMapper/test5/dataReferenceMapping.output.ttl");
	}

	@Test
	public void testSimpleTemplateMapping() {
		testMapping("RmlMapper",
					"RmlMapper/test4/simpleTemplateMapping.rml.ttl",
					"RmlMapper/test4/simpleTemplateMapping.output.ttl");
	}

	@Test
	public void testSimpleReferenceMapping() {
		testMapping("RmlMapper",
					"RmlMapper/test3/simpleReferenceMapping.rml.ttl",
					"RmlMapper/test3/simpleReferenceMapping.output.ttl");
	}

	@Test
	public void testSubjectMapping() {
		testMapping("RmlMapper",
					"RmlMapper/test2/subjectMapping.rml.ttl",
					"RmlMapper/test2/subjectMapping.output.ttl");
	}

	@Test
	public void testXmlResolver() {
		testMapping("RmlMapper",
				"RmlMapper/xmlTest/mapping.ttl",
				"RmlMapper/xmlTest/expected-output.ttl");
	}

	@Test
	public void testCsvMapping() {
		testMapping("RmlMapper",
					"RmlMapper/csv/cars.rml.ttl",
					"RmlMapper/csv/cars.output.ttl");
	}

	@Test
	public void testCsvEuMapping() {
		testMapping("RmlMapper",
					"RmlMapper/csv/cars-eu.rml.ttl",
					"RmlMapper/csv/cars-eu.output.ttl");
	}

	@Test
	public void testCsvPipeDelimeterMapping() {
		testMapping("RmlMapper",
					"RmlMapper/csv/cars-pipe.rml.ttl",
					"RmlMapper/csv/cars.output.ttl");
	}

	//TODO: PM: add test for rml:reference and rr:template where a value is not found.

}
