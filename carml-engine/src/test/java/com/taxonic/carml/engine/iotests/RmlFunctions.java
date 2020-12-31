package com.taxonic.carml.engine.iotests;

import com.google.common.collect.ImmutableList;
import com.taxonic.carml.engine.function.FnoFunction;
import com.taxonic.carml.engine.function.FnoParam;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class RmlFunctions {

  private static class Ex {

    final static String

    prefix = "http://example.com/",

        toBoolFunction = prefix + "toBoolFunction",

        startString = prefix + "startString",

        stringParam = prefix + "stringParam",

        removeNonLatinCharsFunction = prefix + "removeNonLatinCharsFunction",

        toLowercase = prefix + "toLowercase",

        sumFunction = prefix + "sumFunction",

        toIntFunction = prefix + "toIntFunction",

        toIntOutput = prefix + "toIntOutput",

        intParam = prefix + "intParam",

        constantListFunction = prefix + "constantListFunction",

        listParamFunction = prefix + "listParamFunction",

        listParam = prefix + "listParam",

        iriFunction = prefix + "iriFunction",

        baseIriParam = prefix + "baseIriParam";
  }

  @FnoFunction(Ex.toBoolFunction)
  public boolean toBoolFunction(@FnoParam(Ex.startString) String startString) {
    return startString.toLowerCase()
        .equals("yes");
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
    return ImmutableList.of(Ex.prefix + "abc", Ex.prefix + "def", Ex.prefix + "ghi");
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
    return SimpleValueFactory.getInstance()
        .createIRI(baseIri + namePart);
  }


  // TODO: PM: Add test for when parameter is not found
  // TODO: PM: Add test for when function returns null

}
