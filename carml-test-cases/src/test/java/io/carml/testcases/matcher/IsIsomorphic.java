package io.carml.testcases.matcher;

import io.carml.util.ModelSerializer;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class IsIsomorphic extends TypeSafeMatcher<Model> {

    private final Model expected;

    private IsIsomorphic(Model expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(Model actual) {
        return Models.isomorphic(actual, expected);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("model isomorphic to:\n").appendText(serialize(expected));
    }

    @Override
    protected void describeMismatchSafely(Model actual, Description mismatchDescription) {
        var missing = new TreeModel(expected);
        missing.removeAll(actual);

        var unexpected = new TreeModel(actual);
        unexpected.removeAll(expected);

        mismatchDescription.appendText("models were not isomorphic");

        if (!missing.isEmpty()) {
            mismatchDescription
                    .appendText("\n\nMissing (expected but not in result):\n")
                    .appendText(serialize(missing));
        }

        if (!unexpected.isEmpty()) {
            mismatchDescription
                    .appendText("\n\nUnexpected (in result but not expected):\n")
                    .appendText(serialize(unexpected));
        }

        if (missing.isEmpty() && unexpected.isEmpty()) {
            mismatchDescription.appendText(" (differences are in blank node identity only)");
        }
    }

    private static String serialize(Model model) {
        return ModelSerializer.serializeAsRdf(model, RDFFormat.NQUADS);
    }

    public static IsIsomorphic isIsomorphicTo(Model expected) {
        return new IsIsomorphic(expected);
    }
}
