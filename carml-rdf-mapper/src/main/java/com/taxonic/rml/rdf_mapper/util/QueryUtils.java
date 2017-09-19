package com.taxonic.rml.rdf_mapper.util;

import java.util.Arrays;
import java.util.Objects;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.util.Repositories;

public final class QueryUtils {
	
	private QueryUtils() {}

	private static final String QUERY_CONSTRUCT_PART = "CONSTRUCT {?s ?p ?o } ";
	private static final String DEFAULT_QUERY = QUERY_CONSTRUCT_PART + "WHERE {?s ?p ?o}";
	private static final String GRAPH_WHERE_PART = "WHERE { GRAPH ?g { ?s ?p ?o } }";
	private static final String REPOSITORY_MSG = "A repository must be provided";
	private static final String SPARQL_QUERY_MSG = "A SPARQL query must be provided";
	
	public static Model getModelFromRepo(Repository repository, String sparqlQuery) {
		Objects.requireNonNull(repository, REPOSITORY_MSG);
		Objects.requireNonNull(repository, SPARQL_QUERY_MSG);
		
		return 
			Repositories.graphQuery(
				repository, 
				sparqlQuery, 
				QueryResults::asModel
			);
	}
	
	public static Model getModelFromRepo(Repository repository, Resource... contexts) {
		Objects.requireNonNull(repository, REPOSITORY_MSG);
		
		return getModelFromRepo(repository, buildQuery(contexts));
	}
	
	private static String buildQuery(Resource... contexts) {
		return contexts.length > 0 ? buildGraphQuery(contexts) : DEFAULT_QUERY;
	}
	
	private static String buildGraphQuery(Resource... contexts) {
		String fromNameds = Arrays.asList(contexts).stream()
				.map(c -> String.format("FROM NAMED <%s>%n", c))
				.reduce("", String::concat);
		
		return QUERY_CONSTRUCT_PART + fromNameds + GRAPH_WHERE_PART;
	}

}
