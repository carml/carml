package com.taxonic.rml.rdf_mapper.util;

import static java.util.Objects.requireNonNull;

import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.Repository;

import com.taxonic.rml.rdf_mapper.Mapper;
import com.taxonic.rml.rdf_mapper.impl.MapperImpl;

public class RdfObjectLoader {
	
	private static final String RESOURCE_SELECTOR_MSG = "A resource selector must be provided";
	private static final String CLASS_MSG = "A class must be provided";
	private static final String MODEL_MSG = "A model must be provided";
	private static final String MODEL_ADAPTER_MSG = "A model adapter must be provided";
	private static final String REPOSITORY_MSG = "A repository must be provided";
	private static final String SPARQL_QUERY_MSG = "A SPARQL query must be provided";
		
	private RdfObjectLoader() {}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,
			Model model
		) {

		requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		requireNonNull(clazz, CLASS_MSG);
		requireNonNull(model, MODEL_MSG);

		return load(resourceSelector, clazz, model, m -> m);
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,
			Model model,
			UnaryOperator<Model> modelAdapter
		) {
		
		requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		requireNonNull(clazz, CLASS_MSG);
		requireNonNull(model, MODEL_MSG);
		requireNonNull(modelAdapter, MODEL_ADAPTER_MSG);
		
		Mapper mapper = new MapperImpl();
		Set<Resource> resources = resourceSelector.apply(model);
		
		return resources
			.stream()
			.<T> map(r -> mapper.map(modelAdapter.apply(model), r, clazz))
			.collect(ImmutableCollectors.toImmutableSet());
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,	
			Repository repository, 
			String sparqlQuery
		) {
		
		requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		requireNonNull(clazz, CLASS_MSG);
		requireNonNull(repository, REPOSITORY_MSG);
		requireNonNull(sparqlQuery, SPARQL_QUERY_MSG);
		
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
		
		requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		requireNonNull(clazz, CLASS_MSG);
		requireNonNull(repository, REPOSITORY_MSG);
		requireNonNull(sparqlQuery, SPARQL_QUERY_MSG);
		requireNonNull(modelAdapter, MODEL_ADAPTER_MSG);
		
		Model model = QueryUtils.getModelFromRepo(repository, sparqlQuery);
		
		return load(resourceSelector, clazz, model, modelAdapter);		
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,
			Repository repository, 
			Resource... contexts
		) {
		
		requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		requireNonNull(clazz, CLASS_MSG);
		requireNonNull(repository, REPOSITORY_MSG);
		
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
		
		requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		requireNonNull(clazz, CLASS_MSG);
		requireNonNull(repository, REPOSITORY_MSG);
		requireNonNull(modelAdapter, MODEL_ADAPTER_MSG);
		
		Model model = QueryUtils.getModelFromRepo(repository, contexts);
		
		return load(resourceSelector, clazz, model, modelAdapter);		
	}

}
