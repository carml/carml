package io.carml.engine.iotests;

import io.carml.engine.function.FnoFunction;
import io.carml.engine.function.FnoParam;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class RmlFunctions {

    private static class Ex {

        static final String prefix = "http://example.com/";

        static final String toBoolFunction = prefix + "toBoolFunction";

        static final String startString = prefix + "startString";

        static final String stringParam = prefix + "stringParam";

        static final String removeNonLatinCharsFunction = prefix + "removeNonLatinCharsFunction";

        static final String toLowercase = prefix + "toLowercase";

        static final String sumFunction = prefix + "sumFunction";

        static final String toIntFunction = prefix + "toIntFunction";

        static final String toIntOutput = prefix + "toIntOutput";

        static final String intParam = prefix + "intParam";

        static final String constantListFunction = prefix + "constantListFunction";

        static final String listParamFunction = prefix + "listParamFunction";

        static final String listParam = prefix + "listParam";

        static final String iriFunction = prefix + "iriFunction";

        static final String baseIriParam = prefix + "baseIriParam";
    }

    @FnoFunction(Ex.toBoolFunction)
    public boolean toBoolFunction(@FnoParam(Ex.startString) String startString) {
        return startString.toLowerCase().equals("yes");
    }

    @FnoFunction(Ex.removeNonLatinCharsFunction)
    public String removeNonLatinCharsFunction(@FnoParam(Ex.startString) String inputString) {
        return inputString.replaceAll("[^A-Za-z0-9]", "");
    }

    @FnoFunction(Ex.toLowercase)
    public String toLowercase(@FnoParam(Ex.startString) String inputString) {
        return inputString.toLowerCase();
    }

    @FnoFunction(Ex.toIntFunction)
    public int toIntFunction(@FnoParam(Ex.stringParam) String inputString) {
        return Integer.parseInt(inputString);
    }

    @FnoFunction(Ex.sumFunction)
    public int sumFunction(@FnoParam(Ex.toIntOutput) int toIntOutput, @FnoParam(Ex.intParam) int inputInt) {
        return toIntOutput + inputInt;
    }

    @FnoFunction(Ex.constantListFunction)
    public List<String> constantListFunction() {
        return List.of(Ex.prefix + "abc", Ex.prefix + "def", Ex.prefix + "ghi");
    }

    @FnoFunction(Ex.listParamFunction)
    public List<String> listParamFunction(@FnoParam(Ex.listParam) List<String> listParam) {
        return listParam;
    }

    @FnoFunction(Ex.iriFunction)
    public IRI iriFunction(@FnoParam(Ex.baseIriParam) String baseIri, @FnoParam(Ex.stringParam) String namePart) {
        if (StringUtils.isEmpty(namePart)) {
            return null;
        }
        return SimpleValueFactory.getInstance().createIRI(baseIri + namePart);
    }

    // TODO: PM: Add test for when parameter is not found
    // TODO: PM: Add test for when function returns null

}
