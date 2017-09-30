package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.model.impl.CarmlStream;
import com.taxonic.carml.model.impl.LogicalSourceImpl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RmlMapperTest {

	RmlMapper mapper;
	NameableStream stream;
	final String input = "test input";
	final String secondInput = "second test input";
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	@Before
	public void prepareMapper() {
		mapper = RmlMapper.newBuilder().build();
	}

	@Test
	public void mapper_boundWithUnnamedInputStream_shouldReadInputStream() {
		stream = new CarmlStream();
		InputStream inputStream = IOUtils.toInputStream(input);
		mapper.bindInputStream(inputStream);
		assertThat(mapper.readSource(stream), is(input));
	}
	
	@Test
	public void mapper_boundWithNamedInputStream_shouldReadCorrespondingInputStream() {
		String streamName = "foo";
		stream = new CarmlStream(streamName);
		InputStream inputStream = IOUtils.toInputStream(input);
		mapper.bindInputStream(streamName, inputStream);
		assertThat(mapper.readSource(stream), is(input));
	}
	
	@Test
	public void mapper_boundWithUnnamedInputStream_shouldThrowErrorOnUnknownStream() {
		String streamName = "foo";
		stream = new CarmlStream(streamName);
		InputStream inputStream = IOUtils.toInputStream(input);
		mapper.bindInputStream(inputStream);
		
		exception.expect(RuntimeException.class);
		exception.expectMessage(String.format("attempting to get input stream by "
				+ "name [%s], but no such binding is present", streamName));
		
		mapper.readSource(stream);
	}
	
	@Test
	public void mapper_boundWithNamedInputStream_shouldThrowErrorOnUnknownStream() {
		String streamName = "foo";
		String unknownStreamName = "bar";
		stream = new CarmlStream(unknownStreamName);
		InputStream inputStream = IOUtils.toInputStream(input);
		mapper.bindInputStream(streamName, inputStream);
		
		exception.expect(RuntimeException.class);
		exception.expectMessage(String.format("attempting to get input stream by "
				+ "name [%s], but no such binding is present", unknownStreamName));
		
		assertThat(mapper.readSource(stream), is(input));
	}
	
	@Test
	public void mapper_boundWithMultipleNamedInputStreams_shouldReadCorrespondingStreams() {
		String streamName = "foo";
		String secondStreamName = "bar";
		stream = new CarmlStream(streamName);
		NameableStream secondStream = new CarmlStream(secondStreamName);
		InputStream inputStream = IOUtils.toInputStream(input);
		String secondInput = "second test input";
		InputStream secondInputStream = IOUtils.toInputStream(secondInput);
		mapper.bindInputStream(streamName, inputStream);
		mapper.bindInputStream(secondStreamName, secondInputStream);
		assertThat(mapper.readSource(stream), is(input));
		assertThat(mapper.readSource(secondStream), is(secondInput));
	}
	
	@Test
	public void mapper_boundWithMultipleNamedAndOneUnnamedInputStreams_shouldReadCorrespondingStreams() {
		String streamName = "foo";
		String secondStreamName = "bar";
		stream = new CarmlStream(streamName);
		NameableStream secondStream = new CarmlStream(secondStreamName);
		NameableStream unnamedStream = new CarmlStream();
		InputStream inputStream = IOUtils.toInputStream(input);
		InputStream secondInputStream = IOUtils.toInputStream(secondInput);
		String unnamedInput = "unnamed test input";
		InputStream unnamedInputStream = IOUtils.toInputStream(unnamedInput);
		mapper.bindInputStream(streamName, inputStream);
		mapper.bindInputStream(secondStreamName, secondInputStream);
		mapper.bindInputStream(unnamedInputStream);
		assertThat(mapper.readSource(stream), is(input));
		assertThat(mapper.readSource(secondStream), is(secondInput));
		assertThat(mapper.readSource(unnamedStream), is(unnamedInput));
	}
	
	@Test
	public void mapper_boundWithMultipleUnnamedInputStreams_shouldReadLastBoundStream() {
		InputStream inputStream = IOUtils.toInputStream(input);
		InputStream secondInputStream = IOUtils.toInputStream(secondInput);
		mapper.bindInputStream(inputStream);
		assertThat(mapper.readSource(new CarmlStream()), is(input));
		mapper.bindInputStream(secondInputStream);
		assertThat(mapper.readSource(new CarmlStream()), is(secondInput));
	}
	
	@Test
	public void mapper_boundWithMultipleNamedInputStreams_shouldReadLastBoundStream() {
		String streamName = "foo";
		String secondStreamName = "bar";
		InputStream inputStream = IOUtils.toInputStream(input);
		InputStream secondInputStream = IOUtils.toInputStream(secondInput);
		mapper.bindInputStream(streamName, inputStream);
		assertThat(mapper.readSource(new CarmlStream(streamName)), is(input));
		mapper.bindInputStream(secondStreamName, secondInputStream);
		assertThat(mapper.readSource(new CarmlStream(secondStreamName)), is(secondInput));
	}
	
	@Test
	public void mapper_notFindingBoundUnnamedInputStream_shouldThrowException() {
		exception.expect(RuntimeException.class);
		exception.expectMessage("attempting to get the bound input stream, but no binding was present");
		mapper.readSource(new CarmlStream());
	}
	
	@Test
	public void mapper_notFindingBoundNamedInputStreams_shouldThrowException() {
		String streamName = "foo";
		exception.expect(RuntimeException.class);
		exception.expectMessage(String.format("attempting to get input stream by "
				+ "name [%s], but no such binding is present", streamName));
		mapper.readSource(new CarmlStream(streamName));
	}
	
	@Test
	public void mapper_boundWithUnresettableInputStream_shouldStillResolve() throws IOException {
		File file = new File(RmlMapperTest.class.getResource("fileForFileInputStream.txt").getFile());
		String fileContents = FileUtils.readFileToString(file);
		InputStream inputStream = new FileInputStream(file);
		assertThat(inputStream.markSupported(), is(false));
		mapper.bindInputStream(inputStream);
		assertThat(mapper.readSource(new CarmlStream()), is(fileContents));
	}
	
	@Test
	public void mapper_builtWithFileResolver_shouldResolveFile() throws IOException {
		String fileName = "fileForFileInputStream.txt";
		File file = new File(RmlMapperTest.class.getResource(fileName).getFile());
		Path basePath = Paths.get(file.getParent());
		RmlMapper fileMapper = RmlMapper.newBuilder().fileResolver(basePath).build();
		String fileContents = FileUtils.readFileToString(file);
		assertThat(fileMapper.readSource(fileName), is(fileContents));
	}
	
	@Test
	public void mapper_withNoBoundSource_shouldThrowException() throws IOException {
		String source = "some source";
		exception.expect(RuntimeException.class);
		exception.expectMessage(String.format("could not resolve source [%s]", source));
		mapper.readSource(source);
	}

	@Test
	public void mapper_builtWithLogicalSoucreResolvers_shouldUseTheCorrectResolver() throws IOException {
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
		InputStream input = new ByteArrayInputStream("foo".getBytes());
		when(sourceResolver.apply(any())).thenReturn(Optional.of(input));

		RmlMapper mapper = RmlMapper.newBuilder()
				.sourceResolver(sourceResolver)
				.setLogicalSourceResolver(expectedIRI, expectedResolver)
				.setLogicalSourceResolver(unusedIRI, unusedResolver)
				.build();

		TriplesMap logicalSourceMap = mock(TriplesMap.class);
		when(logicalSourceMap.getLogicalSource())
				.thenReturn(new LogicalSourceImpl(null, null, expectedIRI));

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
		InputStream input = new ByteArrayInputStream("foo".getBytes());
		when(sourceResolver.apply(any())).thenReturn(Optional.of(input));

		RmlMapper mapper = RmlMapper.newBuilder()
				.sourceResolver(sourceResolver)
				.setLogicalSourceResolver(unusedIRI, unusedResolver)
				.build();

		IRI unsupportedRefFormulation = f.createIRI("http://this.iri/isNotSupported");

		TriplesMap logicalSourceMap = mock(TriplesMap.class);
		when(logicalSourceMap.getLogicalSource())
				.thenReturn(new LogicalSourceImpl(null, null, unsupportedRefFormulation));


		exception.expect(RuntimeException.class);
		exception.expectMessage(startsWith("Unsupported reference formulation"));
		exception.expectMessage(contains(unsupportedRefFormulation.toString()));

		mapper.getTriplesMapperComponents(logicalSourceMap);
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
	}
}
