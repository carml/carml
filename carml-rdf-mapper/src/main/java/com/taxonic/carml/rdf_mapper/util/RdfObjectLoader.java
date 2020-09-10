package com.taxonic.carml.rdf_mapper.util;

import static java.util.Objects.requireNonNull;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.Repository;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.impl.CarmlMapper;
import com.taxonic.carml.rdf_mapper.impl.MappingCache;

public class RdfObjectLoader {
	
	private static final String
		RESOURCE_SELECTOR_MSG = "A resource selector must be provided",
		CLASS_MSG = "A class must be provided",
		MODEL_MSG = "A model must be provided",
		MODEL_ADAPTER_MSG = "A model adapter must be provided",
		REPOSITORY_MSG = "A repository must be provided",
		SPARQL_QUERY_MSG = "A SPARQL query must be provided",
		POPULATE_CACHE_MSG = "A cache populator must be provided";
		
	private RdfObjectLoader() {}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,
			Model model
		) {
		
		return load(resourceSelector, clazz, model, m -> m, c -> {}, m -> {});
	}
	
	public static <T> Set<T> load(
		Function<Model, Set<Resource>> resourceSelector, 
		Class<T> clazz,
		Model model,
		UnaryOperator<Model> modelAdapter
	) {
		return load(resourceSelector, clazz, model, modelAdapter, c -> {}, m -> {});
	}
		
	public static <T> Set<T> load(
		Function<Model, Set<Resource>> resourceSelector, 
		Class<T> clazz,
		Model model,
		UnaryOperator<Model> modelAdapter,
		Consumer<MappingCache> populateCache,
		Consumer<Mapper> configureMapper
	) {
		return load(resourceSelector, clazz, model, modelAdapter, populateCache, configureMapper, new HashSet<>());
	}

	public static <T> Set<T> load(
			Function<Model, Set<Resource>> resourceSelector,
			Class<T> clazz,
			Model model,
			UnaryOperator<Model> modelAdapter,
			Consumer<MappingCache> populateCache,
			Consumer<Mapper> configureMapper,
			Set<Namespace> namespaces
	) {

		requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		requireNonNull(clazz, CLASS_MSG);
		requireNonNull(model, MODEL_MSG);
		requireNonNull(modelAdapter, MODEL_ADAPTER_MSG);
		requireNonNull(populateCache, POPULATE_CACHE_MSG);

		CarmlMapper mapper = new CarmlMapper(namespaces);
		populateCache.accept(mapper);
		configureMapper.accept(mapper);

		Set<Resource> resources = resourceSelector.apply(model);

		return resources
				.stream()
				.<T> map(r -> mapper.map(modelAdapter.apply(model), r, ImmutableSet.of(clazz)))
				.collect(ImmutableCollectors.toImmutableSet());
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,	
			Repository repository, 
			String sparqlQuery
		) {
		
		return load(resourceSelector, clazz, repository, sparqlQuery, m -> m);
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,	
			Repository repository, 
			String sparqlQuery,
			UnaryOperator<Model> modelAdapter
		) {
		
		requireNonNull(repository, REPOSITORY_MSG);
		requireNonNull(sparqlQuery, SPARQL_QUERY_MSG);
		
		Model model = QueryUtils.getModelFromRepo(repository, sparqlQuery);
		
		return load(resourceSelector, clazz, model, modelAdapter, c -> {}, m -> {});		
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,
			Repository repository, 
			Resource... contexts
		) {
		
		return load(resourceSelector, clazz, repository, m -> m, contexts);		
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,
			Repository repository,
			UnaryOperator<Model> modelAdapter, 
			Resource... contexts
		) {
		
		requireNonNull(repository, REPOSITORY_MSG);
		
		Model model = QueryUtils.getModelFromRepo(repository, contexts);
		
		return load(resourceSelector, clazz, model, modelAdapter, c -> {}, m -> {});		
	}

}
