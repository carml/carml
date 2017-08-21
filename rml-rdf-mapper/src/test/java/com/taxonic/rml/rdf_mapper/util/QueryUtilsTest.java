package com.taxonic.rml.rdf_mapper.util;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryUtilsTest {
	
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final String REPO_CONTEXTS = "Person-Split.trig";
	private static final int REPO_CONTEXTS_NR_STATEMENTS = 22;
	private static final int REPO_CONTEXT_A_NR_STATEMENTS = 8;
	private static final int REPO_CONTEXT_C_NR_STATEMENTS = 9;
	private static final String EX = "http://example.org/";
	private static final String CONTEXT_A = EX + "A" ;
	private static final String CONTEXT_C = EX + "C" ;
	
	private Repository repo;
	
	@Before
	public void setupRepo() 
			throws RDFParseException, UnsupportedRDFormatException, IOException {
		
		repo = new SailRepository(new MemoryStore());
		repo.initialize();
		
		try (RepositoryConnection conn = repo.getConnection()) {			
			
			try (InputStream input = 
					RdfObjectLoaderTest.class.getResourceAsStream(REPO_CONTEXTS)) {
				
				conn.add(input, "", RDFFormat.TRIG);
			}
			
			try (RepositoryResult<Statement> result = conn.getStatements(null, null, null)) {
				assertThat(
					"The in-memory store may not be empty",  
					Iterations.asSet(result),
					is(not(empty()))
				);
			}
			
		}
	}
	
	@After
	public void shutdownRepo() {
		repo.shutDown();
	}
	
	@Test
	public void modelGetter_givenSparqlQuery_ShouldReturnAllCorrespondingStatements() {
		String sparqlQuery = ""
				+ "CONSTRUCT {"
				+ "  ?s <http://schema.org/name> ?name . "
				+ "  ?s <http://schema.org/gender> ?gender "
				+ "} "
				+ "FROM NAMED <http://example.org/A>" 
				+ "WHERE { "
				+ "  GRAPH ?g { "
				+ "    ?s <http://schema.org/name> ?name . "
				+ "    ?s <http://schema.org/gender> ?gender "
				+ "  } "
				+ "}";
		
		Model model = QueryUtils.getModelFromRepo(repo, sparqlQuery);
		assertThat(
			"All statements corresponding to sparql query should be loaded",
			model,
			hasSize(2)
		);
	}
	
	@Test
	public void modelGetter_givenSpecifiContexts_shouldLoadAllStatementsInRepo() {
		Model model = QueryUtils.getModelFromRepo(repo);
		assertThat(
			String.format("The in-memory store should contain %s statemtents", 
					REPO_CONTEXTS_NR_STATEMENTS), 
			model, 
			hasSize(REPO_CONTEXTS_NR_STATEMENTS)
		);
	}
	
	@Test
	public void modelGetter_givenNoContext_shouldReturnAllStatementsInContexts() {
		Model model = 
			QueryUtils.getModelFromRepo(
				repo,
				VF.createIRI(CONTEXT_A),
				VF.createIRI(CONTEXT_C)
			);
		
		assertThat(
			String.format("The in-memory store should contain %s statemtents", 
					REPO_CONTEXT_A_NR_STATEMENTS + REPO_CONTEXT_C_NR_STATEMENTS), 
			model, 
			hasSize(REPO_CONTEXT_A_NR_STATEMENTS + REPO_CONTEXT_C_NR_STATEMENTS)
		);
	}
	
	@Test
	public void modelGetter_givenSingleContext_shouldLoadAllStatementsOfContext() {
		Model model = QueryUtils.getModelFromRepo(repo, VF.createIRI(CONTEXT_A));
		
		assertThat(
				String.format("The in-memory store should contain %s statemtents", 
						REPO_CONTEXT_A_NR_STATEMENTS), 
				model, 
				hasSize(REPO_CONTEXT_A_NR_STATEMENTS)
			);
	}
	
}
