package com.taxonic.carml.rdf_mapper.impl;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;

class IterablePropertyValueMapper implements PropertyValueMapper {

	private ValueTransformer valueTransformer;
	private Supplier<Collection<Object>> createIterable;
	private UnaryOperator<Collection<Object>> immutableTransform;

	public IterablePropertyValueMapper(
		ValueTransformer valueTransformer,
		Supplier<Collection<Object>> createIterable,
		UnaryOperator<Collection<Object>> immutableTransform
	) {
		this.valueTransformer = valueTransformer;
		this.createIterable = createIterable;
		this.immutableTransform = immutableTransform;
	}

	@Override
	public Optional<Object> map(Model model, Object instance, List<Value> values) {
		
		List<Object> transformed = values.stream()
				.map(v -> valueTransformer.transform(model, v))
				.collect(toList());

		Collection<Object> result = createIterable.get();
		transformed.forEach(result::add);
		Collection<Object> immutable = immutableTransform.apply(result);
		return Optional.of(immutable);
		
	}

	public static IterablePropertyValueMapper createForIterableType(
		ValueTransformer valueTransformer,
		Class<?> iterableType
	) {
		Objects.requireNonNull(iterableType);
		
		Supplier<Collection<Object>> createIterable = createCollectionFactory(iterableType);
		UnaryOperator<Collection<Object>> immutableTransform = createImmutableTransform(iterableType);

		return
		new IterablePropertyValueMapper(
			valueTransformer,
			createIterable,
			immutableTransform
		);
	}
	
	private static Supplier<Collection<Object>> createCollectionFactory(Class<?> iterableType) {
		// TODO Map<Class<?>, Supplier<Collection<Object>>>
		if (iterableType.equals(Set.class)) {
			return LinkedHashSet::new;
		} else if (iterableType.equals(List.class)) {
			return LinkedList::new;
		}
		throw new RuntimeException("don't know how to create a factory for collection type [" + iterableType.getCanonicalName() + "]");
	}

	private static UnaryOperator<Collection<Object>> createImmutableTransform(Class<?> iterableType) {
		if (iterableType.equals(Set.class)) {
			return x -> Collections.unmodifiableSet((Set<Object>) x);
		} else if (iterableType.equals(List.class)) {
			return x -> Collections.unmodifiableList((List<Object>) x);
		}
		throw new RuntimeException("don't know how to create a transform to make collections of type [" + iterableType.getCanonicalName() + "] immutable");
	}
	
}
