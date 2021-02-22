package com.taxonic.carml.logical_source_resolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.Iterables;
import com.jayway.jsonpath.InvalidPathException;
import com.taxonic.carml.engine.Item;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

public class JsonPathResolverTest {

    private static final String SOURCE =
            "{\n" +
            "  \"food\": [\n" +
            "    {\n" +
            "      \"name\": \"Belgian Waffles\",\n" +
            "      \"countryOfOrigin\": \"Belgium\",\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"French Toast\",\n" +
            "      \"countryOfOrigin\": \"France\",\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"Dutch Pancakes\",\n" +
            "      \"countryOfOrigin\": \"Netherlands\",\n" +
            "    }\n" +
            "  ]\n" +
            "}" ;

    private JsonPathResolver jsonPathResolver;

    private final Function<Object, String> sourceResolver = s -> s.toString();

    @Before
    public void init() {
        jsonPathResolver = new JsonPathResolver();
    }

    @Test
    public void sourceIterator_givenJsonPath_shouldReturnMatchingObjects() {
        LogicalSource foodSource =
                new CarmlLogicalSource(SOURCE, "food[*]", Rdf.Ql.JsonPath);
        Stream<Item<Object>> objectStream = jsonPathResolver.bindSource(foodSource, sourceResolver).get();

        assertThat(objectStream.count(), is(3L));
    }

    @Test
    public void sourceIterator_givenUnresolvableJsonPath_shouldReturnEmptyIterable() {
        LogicalSource unresolvable = new CarmlLogicalSource(SOURCE, "foo", Rdf.Ql.JsonPath);
        Stream<Item<Object>> objectStream = jsonPathResolver.bindSource(unresolvable, sourceResolver).get();

        assertThat(objectStream.count(), is(0L));
    }

    @Test
    public void sourceIterator_givenInvalidJsonPath_shouldThrowException() {
        LogicalSource unresolvable = new CarmlLogicalSource(SOURCE, "food[invalid]", Rdf.Ql.JsonPath);
        InvalidPathException exception = assertThrows(InvalidPathException.class,
                () -> jsonPathResolver.bindSource(unresolvable, sourceResolver).get());

        assertThat(exception.getMessage(), startsWith("Could not parse"));
    }
}
