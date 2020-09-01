package com.taxonic.carml.rmltestcases;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;

import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.logical_source_resolver.CsvResolver;
import com.taxonic.carml.logical_source_resolver.JsonPathResolver;
import com.taxonic.carml.logical_source_resolver.XPathResolver;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import com.taxonic.carml.rdf_mapper.util.RdfObjectLoader;
import com.taxonic.carml.rmltestcases.model.Dataset;
import com.taxonic.carml.rmltestcases.model.Output;
import com.taxonic.carml.rmltestcases.model.TestCase;
import com.taxonic.carml.util.IoUtils;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf;

@RunWith(Parameterized.class)
public class TestRmlTestCases {

	static final String CLASS_LOCATION = "com/taxonic/carml/rmltestcases/test-cases";

	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	static final IRI EARL_TESTCASE = VF.createIRI("http://www.w3.org/ns/earl#TestCase");

	static final List<String> SUPPORTED_SOURCE_TYPES = ImmutableList.of("CSV", "JSON", "XML");

	// Under discussion in https://github.com/RMLio/rml-test-cases/issues
	private static final List<String> SKIP_TESTS = new ImmutableList.Builder<String>() //
			.add("RMLTC0002c-JSON") //
			.add("RMLTC0002c-XML") //
			.add("RMLTC0007h-CSV") //
			.add("RMLTC0007h-JSON") //
			.add("RMLTC0007h-XML") //
			.add("RMLTC0010a-JSON") //
			.add("RMLTC0010b-JSON") //
			.add("RMLTC0010c-JSON") //
			.add("RMLTC0015b-CSV") //
			.add("RMLTC0015b-JSON") //
			.add("RMLTC0015b-XML") //
			.add("RMLTC0019b-CSV") //
			.add("RMLTC0019b-JSON") //
			.add("RMLTC0019b-XML") //
			.add("RMLTC0020b-CSV") //
			.add("RMLTC0020b-JSON") //
			.add("RMLTC0020b-XML") //
			.build();

	private RmlMapper mapper;

	@Parameter
	public TestCase testCase;

	@Parameters(name = "{0}")
	public static Set<TestCase> populateTestCases() {
		InputStream metadata = TestRmlTestCases.class.getResourceAsStream("test-cases/metadata.nt");
		return RdfObjectLoader.load(selectTestCases, RmlTestCase.class, IoUtils.parse(metadata, RDFFormat.NTRIPLES)) //
				.stream() //
				.filter(TestRmlTestCases::shouldBeTested) //
				.collect(ImmutableCollectors.toImmutableSet());
	}

	private static Function<Model, Set<Resource>> selectTestCases = //
			model -> ImmutableSet.copyOf(model.filter(null, RDF.TYPE, EARL_TESTCASE)//
							.subjects() //
							.stream() //
							.filter(TestRmlTestCases::isSupported) //
							.collect(ImmutableCollectors.toImmutableSet()));

	private static boolean isSupported(Resource resource) {
		return SUPPORTED_SOURCE_TYPES.stream()//
				.anyMatch(s -> resource.stringValue().endsWith(s));
	}

	private static boolean shouldBeTested(TestCase testCase) {
		return !SKIP_TESTS.contains(testCase.getIdentifier());
	}

	@Before
	public void prepare() {
		mapper = RmlMapper.newBuilder() //
				.setLogicalSourceResolver(Rdf.Ql.JsonPath, new JsonPathResolver()) //
				.setLogicalSourceResolver(Rdf.Ql.XPath, new XPathResolver()) //
				.setLogicalSourceResolver(Rdf.Ql.Csv, new CsvResolver()) //
				.classPathResolver(String.format("%s/%s", CLASS_LOCATION, testCase.getIdentifier())) //
				.build();
	}

	@Test
	public void runTestCase() {
		Output expectedOutput = testCase.getOutput();
		if (expectedOutput.isError()) {
			assertThrows(RuntimeException.class, () -> executeMapping());
		} else {
			Model result = executeMapping();
			InputStream expectedOutputStream = getDatasetInputStream(expectedOutput);

			Model expected = IoUtils.parse(expectedOutputStream, RDFFormat.NQUADS) //
					.stream() //
					.collect(Collectors.toCollection(TreeModel::new));

			assertThat(result, is(expected));
		}
	}

	private Model executeMapping() {
		InputStream mappingStream = getDatasetInputStream(testCase.getRules());
		Set<TriplesMap> mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

		return mapper.map(mapping) //
				.stream() //
				.collect(Collectors.toCollection(TreeModel::new));
	}

	static InputStream getDatasetInputStream(Dataset dataset) {
		String relativeLocation = dataset.getDistribution().getRelativeFileLocation();
		return TestRmlTestCases.class.getResourceAsStream(relativeLocation);
	}
}
