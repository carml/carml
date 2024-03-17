package io.carml.rdfmapper.impl;

import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfType;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Setter;

@Setter
public class Person {

    private String name;

    private PostalAddress address;

    private Set<Person> knows;

    private List<Person> friends;

    static final String SCHEMAORG = "http://schema.org/";

    static final String SCHEMAORG_NAME = SCHEMAORG + "name";

    static final String SCHEMAORG_ADDRESS = SCHEMAORG + "address";

    static final String SCHEMAORG_KNOWS = SCHEMAORG + "knows";

    static final String SCHEMAORG_RELATED_TO = SCHEMAORG + "relatedTo";

    static final String SCHEMAORG_COLLEAGUES = SCHEMAORG + "colleagues";

    static final String EX_FRIENDS = "http://example.org/friends";

    @RdfProperty(SCHEMAORG_NAME)
    public Optional<String> getName() {
        return Optional.ofNullable(name);
    }

    @RdfProperty(SCHEMAORG_ADDRESS)
    @RdfType(PostalAddress.class)
    public PostalAddress getAddress() {
        return address;
    }

    @RdfProperty(SCHEMAORG_KNOWS)
    @RdfProperty(SCHEMAORG_RELATED_TO)
    @RdfProperty(value = SCHEMAORG_COLLEAGUES, deprecated = true)
    @RdfType(Person.class)
    public Set<Person> getKnows() {
        return knows;
    }

    public void setKnows(Set<Person> knows) {
        if (this.knows == null) {
            this.knows = new HashSet<>();
        }
        this.knows.addAll(knows);
    }

    @RdfProperty(EX_FRIENDS)
    @RdfType(Person.class)
    public List<Person> getFriends() {
        return friends;
    }

    @Override
    public String toString() {
        return "Person [name=" + name + ", address=" + address + ", knows=" + knows + "]";
    }
}
