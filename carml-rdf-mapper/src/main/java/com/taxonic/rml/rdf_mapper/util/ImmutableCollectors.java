package com.taxonic.rml.rdf_mapper.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.stream.Collector;

public class ImmutableCollectors {
	
	private ImmutableCollectors() {}
	
	public static <T> Collector<T, ImmutableList.Builder<T>, ImmutableList<T>> toImmutableList() {
		return Collector.of(
			ImmutableList.Builder<T>::new,
			ImmutableList.Builder<T>::add,
			(left, right) -> left.addAll(right.build()),
			ImmutableList.Builder<T>::build);
	}
	
	public static <T> Collector<T, ImmutableSet.Builder<T>, ImmutableSet<T>> toImmutableSet() {
		return Collector.of(
			ImmutableSet.Builder<T>::new,
			ImmutableSet.Builder<T>::add,
			(left, right) -> left.addAll(right.build()),
			ImmutableSet.Builder<T>::build);
	}

}
