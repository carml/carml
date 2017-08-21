package com.taxonic.rml.rdf_mapper.util;

import com.taxonic.rml.rdf_mapper.Mapper;
import com.taxonic.rml.rdf_mapper.impl.MapperImpl;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.Repository;

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
		
		Objects.requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		Objects.requireNonNull(clazz, CLASS_MSG);
		Objects.requireNonNull(model, MODEL_MSG);
		
		Mapper mapper = new MapperImpl();
		Set<Resource> resources = resourceSelector.apply(model);
		
		return resources
			.stream()
			.<T> map(r -> mapper.map(model, r, clazz))
			.collect(ImmutableCollectors.toImmutableSet());
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,
			Model model,
			UnaryOperator<Model> modelAdapter
		) {
		
		Objects.requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		Objects.requireNonNull(clazz, CLASS_MSG);
		Objects.requireNonNull(model, MODEL_MSG);
		Objects.requireNonNull(modelAdapter, MODEL_ADAPTER_MSG);
		
		return load(resourceSelector, clazz, modelAdapter.apply(model));
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,	
			Repository repository, 
			String sparqlQuery
		) {
		
		Objects.requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		Objects.requireNonNull(clazz, CLASS_MSG);
		Objects.requireNonNull(repository, REPOSITORY_MSG);
		Objects.requireNonNull(sparqlQuery, SPARQL_QUERY_MSG);
		
		Model model = QueryUtils.getModelFromRepo(repository, sparqlQuery);
		
		return load(resourceSelector, clazz, model);		
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,	
			Repository repository, 
			String sparqlQuery,
			UnaryOperator<Model> modelAdapter
		) {
		
		Objects.requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		Objects.requireNonNull(clazz, CLASS_MSG);
		Objects.requireNonNull(repository, REPOSITORY_MSG);
		Objects.requireNonNull(sparqlQuery, SPARQL_QUERY_MSG);
		Objects.requireNonNull(modelAdapter, MODEL_ADAPTER_MSG);
		
		Model model = QueryUtils.getModelFromRepo(repository, sparqlQuery);
		
		return load(resourceSelector, clazz, modelAdapter.apply(model));		
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,
			Repository repository, 
			Resource... contexts
		) {
		
		Objects.requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		Objects.requireNonNull(clazz, CLASS_MSG);
		Objects.requireNonNull(repository, REPOSITORY_MSG);
		
		Model model = QueryUtils.getModelFromRepo(repository, contexts);
		
		return load(resourceSelector, clazz, model);		
	}
	
	public static <T> Set<T> 
		load(
			Function<Model, Set<Resource>> resourceSelector, 
			Class<T> clazz,
			Repository repository,
			UnaryOperator<Model> modelAdapter, 
			Resource... contexts
		) {
		
		Objects.requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		Objects.requireNonNull(clazz, CLASS_MSG);
		Objects.requireNonNull(repository, REPOSITORY_MSG);
		Objects.requireNonNull(modelAdapter, MODEL_ADAPTER_MSG);
		
		Model model = QueryUtils.getModelFromRepo(repository, contexts);
		
		return load(resourceSelector, clazz, modelAdapter.apply(model));		
	}

}
