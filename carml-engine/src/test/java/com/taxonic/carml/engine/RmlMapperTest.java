package com.taxonic.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.taxonic.carml.logical_source_resolver.CsvResolver;
import com.taxonic.carml.logical_source_resolver.JsonPathResolver;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.logical_source_resolver.XPathResolver;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.model.impl.CarmlStream;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RmlMapperTest {

	RmlMapper mapper;
	NameableStream stream;
	final String input = "test input";
	final String secondInput = "second test input";

	@Before
	public void prepareMapper() {
		mapper = RmlMapper.newBuilder()
				.setLogicalSourceResolver(Rdf.Ql.Csv, new CsvResolver())
				.setLogicalSourceResolver(Rdf.Ql.JsonPath, new JsonPathResolver())
				.setLogicalSourceResolver(Rdf.Ql.XPath, new XPathResolver())
				.build();
	}

	@Test
	public void mapper_boundWithUnnamedInputStream_shouldReadInputStream() throws IOException {
		stream = new CarmlStream();
		InputStream inputStream = IOUtils.toInputStream(input, Charset.defaultCharset());
		mapper.bindInputStream(inputStream);
		assertThat(mapper.getSourceManager().getSource(RmlMapper.DEFAULT_STREAM_NAME), is(input));
	}

	@Test
	public void mapper_boundWithNamedInputStream_shouldReadCorrespondingInputStream() {
		String streamName = "foo";
		stream = new CarmlStream(streamName);
		InputStream inputStream = IOUtils.toInputStream(input, Charset.defaultCharset());
		mapper.bindInputStream(streamName, inputStream);
		assertThat(mapper.getSourceManager().getSource(streamName), is(input));
	}

	@Test
	public void mapper_boundWithUnnamedInputStream_shouldThrowErrorOnUnknownStream() {
		String streamName = "foo";
		stream = new CarmlStream(streamName);
		InputStream inputStream = IOUtils.toInputStream(input, Charset.defaultCharset());
		mapper.bindInputStream(inputStream);

		RmlMappingLoader loader = RmlMappingLoader.build();
		InputStream input = RmlMapperTest.class.getResourceAsStream("simple.namedcarml.rml.ttl");

		RuntimeException exception = assertThrows(RuntimeException.class, () -> mapper.map(loader.load(RDFFormat.TURTLE, input)));
		assertThat(exception.getMessage(), is(String.format("attempting to get source by "
				+ "name [%s], but no such binding is present", streamName)));
	}

	@Test
	public void mapper_boundWithNamedInputStream_shouldThrowErrorOnUnknownStream() {
		String streamName = "bar";
		String unknownStreamName = "foo";
		stream = new CarmlStream(unknownStreamName);
		InputStream inputStream = IOUtils.toInputStream(input, Charset.defaultCharset());
		mapper.bindInputStream(streamName, inputStream);

		RmlMappingLoader loader = RmlMappingLoader.build();
		InputStream input = RmlMapperTest.class.getResourceAsStream("simple.namedcarml.rml.ttl");

		RuntimeException exception = assertThrows(RuntimeException.class, () -> mapper.map(loader.load(RDFFormat.TURTLE, input)));
		assertThat(exception.getMessage(), is(String.format("attempting to get source by "
				+ "name [%s], but no such binding is present", unknownStreamName)));
	}

	@Test
	public void mapper_boundWithMultipleNamedInputStreams_shouldReadCorrespondingStreams() {
		String streamName = "foo";
		String secondStreamName = "bar";
		stream = new CarmlStream(streamName);
		InputStream inputStream = IOUtils.toInputStream(input, Charset.defaultCharset());
		String secondInput = "second test input";
		InputStream secondInputStream = IOUtils.toInputStream(secondInput, Charset.defaultCharset());
		mapper.bindInputStream(streamName, inputStream);
		mapper.bindInputStream(secondStreamName, secondInputStream);
		assertThat(mapper.getSourceManager().getSource(streamName), is(input));
		assertThat(mapper.getSourceManager().getSource(secondStreamName), is(secondInput));
	}

	@Test
	public void mapper_boundWithMultipleNamedAndOneUnnamedInputStreams_shouldReadCorrespondingStreams() {
		String streamName = "foo";
		String secondStreamName = "bar";
		stream = new CarmlStream(streamName);
		InputStream inputStream = IOUtils.toInputStream(input, Charset.defaultCharset());
		InputStream secondInputStream = IOUtils.toInputStream(secondInput, Charset.defaultCharset());
		String unnamedInput = "unnamed test input";
		InputStream unnamedInputStream = IOUtils.toInputStream(unnamedInput, Charset.defaultCharset());
		mapper.bindInputStream(streamName, inputStream);
		mapper.bindInputStream(secondStreamName, secondInputStream);
		mapper.bindInputStream(unnamedInputStream);
		assertThat(mapper.getSourceManager().getSource(streamName), is(input));
		assertThat(mapper.getSourceManager().getSource(secondStreamName), is(secondInput));
		assertThat(mapper.getSourceManager().getSource(RmlMapper.DEFAULT_STREAM_NAME), is(unnamedInput));
	}

	@Test
	public void mapper_boundWithMultipleUnnamedInputStreams_shouldReadLastBoundStream() {
		InputStream inputStream = IOUtils.toInputStream(input, Charset.defaultCharset());
		InputStream secondInputStream = IOUtils.toInputStream(secondInput, Charset.defaultCharset());
		mapper.bindInputStream(inputStream);
		assertThat(mapper.getSourceManager().getSource(RmlMapper.DEFAULT_STREAM_NAME), is(input));
		mapper.bindInputStream(secondInputStream);
		assertThat(mapper.getSourceManager().getSource(RmlMapper.DEFAULT_STREAM_NAME), is(secondInput));
	}

	@Test
	public void mapper_boundWithMultipleNamedInputStreams_shouldReadLastBoundStream() {
		String streamName = "foo";
		String secondStreamName = "bar";
		InputStream inputStream = IOUtils.toInputStream(input, Charset.defaultCharset());
		InputStream secondInputStream = IOUtils.toInputStream(secondInput, Charset.defaultCharset());
		mapper.bindInputStream(streamName, inputStream);
		assertThat(mapper.getSourceManager().getSource(streamName), is(input));
		mapper.bindInputStream(secondStreamName, secondInputStream);
		assertThat(mapper.getSourceManager().getSource(secondStreamName), is(secondInput));
	}

	@Test
	public void mapper_notFindingBoundUnnamedInputStream_shouldThrowException() {
		RmlMappingLoader loader = RmlMappingLoader.build();
		InputStream input = RmlMapperTest.class.getResourceAsStream("simple.carml.rml.ttl");

		RuntimeException exception = assertThrows(RuntimeException.class, () -> mapper.map(loader.load(RDFFormat.TURTLE, input)));
		assertThat(exception.getMessage(), is("attempting to get source, but no binding was present"));
	}

	@Test
	public void mapper_notFindingBoundNamedInputStreams_shouldThrowException() {
		String streamName = "foo";
		RmlMappingLoader loader = RmlMappingLoader.build();
		InputStream input = RmlMapperTest.class.getResourceAsStream("simple.namedcarml.rml.ttl");

		RuntimeException exception = assertThrows(RuntimeException.class, () -> mapper.map(loader.load(RDFFormat.TURTLE, input)));
		assertThat(exception.getMessage(), is(String.format("attempting to get source by "
				+ "name [%s], but no such binding is present", streamName)));
	}

	@Test
	public void mapper_withNoBoundSource_shouldThrowException() throws IOException {
		RmlMappingLoader loader = RmlMappingLoader.build();
		InputStream input = RmlMapperTest.class.getResourceAsStream("simple.carml.rml.ttl");

		RuntimeException exception = assertThrows(RuntimeException.class, () -> mapper.map(loader.load(RDFFormat.TURTLE, input)));
		assertThat(exception.getMessage(), is("attempting to get source, but no binding was present"));
	}

	@Test
	public void mapper_builtWithLogicalSourceResolvers_shouldUseTheCorrectResolver() throws IOException {
		ValueFactory f = SimpleValueFactory.getInstance();
		IRI expectedIRI = f.createIRI("http://this.iri/isUsed");
		Iterable<String> expectedSourceData = Collections.singletonList("expected");
		LogicalSourceResolver.SourceIterator<String> expectedSourceIterator = (a, b) -> expectedSourceData;
		LogicalSourceResolver.ExpressionEvaluatorFactory<String> expectedFactory = a -> null;
		LogicalSourceResolver<String> expectedResolver = new LogicalSourceResolverContainer<>(
				expectedSourceIterator, expectedFactory);

		IRI unusedIRI = f.createIRI("http://this.iri/isNotUsed");
		LogicalSourceResolver<String> unusedResolver = new LogicalSourceResolverContainer<>(null, null);

		SourceResolver sourceResolver = mock(SourceResolver.class);
		String input = "foo";
		when(sourceResolver.apply(any())).thenReturn(Optional.of(input));

		RmlMapper mapper = RmlMapper.newBuilder()
				.sourceResolver(sourceResolver)
				.setLogicalSourceResolver(expectedIRI, expectedResolver)
				.setLogicalSourceResolver(unusedIRI, unusedResolver)
				.build();

		TriplesMap logicalSourceMap = mock(TriplesMap.class);
		when(logicalSourceMap.getLogicalSource())
				.thenReturn(new CarmlLogicalSource(null, null, expectedIRI));

		TriplesMapperComponents<?> components = mapper.getTriplesMapperComponents(logicalSourceMap);

		Assert.assertSame(expectedFactory, components.getExpressionEvaluatorFactory());
		Assert.assertSame(expectedSourceData, components.getIterator().get());

	}

	@Test
	public void mapper_usedWithUnknownReferenceFormulation_shouldThrowException() throws IOException {
		ValueFactory f = SimpleValueFactory.getInstance();
		IRI unusedIRI = f.createIRI("http://this.iri/isNotUsed");
		LogicalSourceResolver<?> unusedResolver = new LogicalSourceResolverContainer<>(null, null);

		SourceResolver sourceResolver = mock(SourceResolver.class);
		String input = "foo";
		when(sourceResolver.apply(any())).thenReturn(Optional.of(input));

		RmlMapper mapper = RmlMapper.newBuilder()
				.sourceResolver(sourceResolver)
				.setLogicalSourceResolver(unusedIRI, unusedResolver)
				.build();

		IRI unsupportedRefFormulation = f.createIRI("http://this.iri/isNotSupported");

		TriplesMap logicalSourceMap = mock(TriplesMap.class);
		when(logicalSourceMap.asRdf())
				.thenReturn(new LinkedHashModel());
		when(logicalSourceMap.getLogicalSource())
				.thenReturn(new CarmlLogicalSource(null, null, unsupportedRefFormulation));

		RuntimeException exception = assertThrows(RuntimeException.class, () -> mapper.getTriplesMapperComponents(logicalSourceMap));
		assertThat(exception.getMessage(), startsWith("Unsupported reference formulation"));
		assertThat(exception.getMessage(), containsString(unsupportedRefFormulation.toString()));


	}

	private static class LogicalSourceResolverContainer<T> implements LogicalSourceResolver<T> {

		SourceIterator<T> sourceIterator;
		ExpressionEvaluatorFactory<T> evaluatorFactory;

		public LogicalSourceResolverContainer(SourceIterator<T> sourceIterator, ExpressionEvaluatorFactory<T> evaluatorFactory) {
			this.sourceIterator = sourceIterator;
			this.evaluatorFactory = evaluatorFactory;
		}

		@Override
		public SourceIterator<T> getSourceIterator() {
			return sourceIterator;
		}

		@Override
		public ExpressionEvaluatorFactory<T> getExpressionEvaluatorFactory() {
			return evaluatorFactory;
		}

		@Override
		public GetIterableFromContext<T> createGetIterableFromContext(String iterator) {
			throw new UnsupportedOperationException();
		}

		@Override
		public CreateContextEvaluate getCreateContextEvaluate() {
			throw new UnsupportedOperationException();
		}
	}
}
