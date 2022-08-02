package io.carml.rdfmapper.util;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("java:S1135")
class RdfObjectLoaderTest {

  private static final ValueFactory VF = SimpleValueFactory.getInstance();

  private static final String REPO_CONTEXTS = "Person-Split.trig";

  private static final String EX = "http://example.org/";

  private static final String CONTEXT_C = EX + "C";

  private Repository repo;

  private Model model;

  private static Function<Model, Set<Resource>> selectAllResources = model -> Set.copyOf(model.subjects());


  private static Function<Model, Set<Resource>> selectPersons =
      model -> Set.copyOf(model.filter(null, RDF.TYPE, VF.createIRI(Person.SCHEMAORG + "Person"))
          .subjects());

  private static Function<Model, Set<Resource>> selectAddresses =
      model -> Set.copyOf(model.filter(null, VF.createIRI(PostalAddress.SCHEMAORG_POSTALCODE), null)
          .subjects());

  private static UnaryOperator<Model> uppercaser = model -> model.stream()
      .map(st -> {
        if (st.getObject() instanceof Literal) {
          st = upperCaseStatementLiteral(st);
        }
        return st;
      })
      .collect(ModelCollector.toModel());

  private static Statement upperCaseStatementLiteral(Statement st) {
    return VF.createStatement(st.getSubject(), st.getPredicate(), VF.createLiteral(StringUtils.upperCase(st.getObject()
        .stringValue())));
  }

  @BeforeEach
  public void loadData() {

    try (InputStream input = RdfObjectLoaderTest.class.getResourceAsStream("Person-and-Event.jsonld")) {

      model = Rio.parse(input, "", RDFFormat.JSONLD);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }



    repo = new SailRepository(new MemoryStore());
    repo.init();

    try (RepositoryConnection conn = repo.getConnection()) {

      try (InputStream input = RdfObjectLoaderTest.class.getResourceAsStream(REPO_CONTEXTS)) {

        conn.add(input, "", RDFFormat.TRIG);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      try (RepositoryResult<Statement> result = conn.getStatements(null, null, null)) {
        assertThat("The in-memory store may not be empty", result.stream()
            .collect(toSet()), is(not(empty())));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }
  }

  @AfterEach
  public void shutdownRepo() {
    repo.shutDown();
  }

  @Test
  void personLoader_givenPersonSelector_shouldLoadFourPeople()
      throws RDFParseException, UnsupportedRDFormatException, IOException {

    Set<Person> people = RdfObjectLoader.load(selectPersons, Person.class, model);
    assertThat("4 people should be loaded", people, hasSize(4));
  }

  @Test
  void personLoader_givenPersonSelector_shouldLoadFourPeopleOneWithAddress()
      throws RDFParseException, UnsupportedRDFormatException, IOException {

    Set<Person> people = RdfObjectLoader.load(selectPersons, Person.class, model);
    int peopleWithAddr = (int) people.stream()
        .filter(p -> p.getAddress() != null)
        .count();

    assertThat("1 person with address should be loaded", peopleWithAddr, is(1));
  }

  @Test
  void addressLoader_givenAddressSelector_shouldLoadOneAddress()
      throws RDFParseException, UnsupportedRDFormatException, IOException {

    Set<PostalAddress> addresses = RdfObjectLoader.load(selectAddresses, PostalAddress.class, model);
    assertThat("1 address should be loaded", addresses.size(), is(1));
  }

  @Test
  void personLoader_givenAllResourcesSelector_shouldLoadEightPeople()
      throws RDFParseException, UnsupportedRDFormatException, IOException {

    Set<Person> people = RdfObjectLoader.load(selectAllResources, Person.class, model);
    assertThat("", people, hasSize(8));
  }

  @Test
  void repoLoader_givenContextAndPersonFilter_shouldNotLoadPersonStamentsInOtherContexts() {
    Set<Person> people = RdfObjectLoader.load(selectPersons, Person.class, repo, VF.createIRI(CONTEXT_C));

    assertThat("3 person resources should be created", people, hasSize(3));
  }

  @Test
  void repoLoader_givenQuery_shouldLoadStamentsCorrespondingToQuery() {
    String sparqlQuery =
        "" + "CONSTRUCT {" + "  ?s <http://schema.org/name> ?name . " + "  ?s <http://schema.org/gender> ?gender "
            + "} " + "FROM NAMED <http://example.org/A>" + "WHERE { " + "  GRAPH ?g { "
            + "    ?s <http://schema.org/name> ?name . " + "    ?s <http://schema.org/gender> ?gender " + "  } " + "}";

    Set<Person> people = RdfObjectLoader.load(selectAllResources, Person.class, repo, sparqlQuery);

    assertThat("1 person resource should be created", people, hasSize(1));

    Person person = Iterables.getOnlyElement(people);

    assertThat("", person.getName(), is("Manu Sporny"));
    assertThat("", person.getGender(), is("male"));
    assertThat("", person.getJobTitle(), nullValue());
    assertThat("", person.getTelephone(), nullValue());
    assertThat("", person.getEmail(), nullValue());
    assertThat("", person.getColleagues(), empty());

  }

  @Test
  void addressLoader_givenAddressSelectorAndUpperCaseAdapter_shouldLoadAddressWithUppercaseLiterals()
      throws RDFParseException, UnsupportedRDFormatException, IOException {

    Set<PostalAddress> addresses = RdfObjectLoader.load(selectAddresses, PostalAddress.class, model, uppercaser);
    PostalAddress address = Iterables.getOnlyElement(addresses);

    assertThat("", address.getStreetAddress(), is("1700 KRAFT DRIVE, SUITE 2408"));
    assertThat("", address.getAddressLocality(), is("BLACKSBURG"));
    assertThat("", address.getAddressRegion(), is("VA"));
    assertThat("", address.getPostalCode(), is("24060"));
  }


  // TODO: PM: add test for populateCache
}
