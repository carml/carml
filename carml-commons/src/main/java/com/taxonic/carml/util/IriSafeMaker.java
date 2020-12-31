package com.taxonic.carml.util;

import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Creates IRI-safe values, by percent-encoding any characters in a {@code String} that are NOT one
 * of the following: - alphabetic, as per {@code Character::isAlphabetic} - a digit, as per
 * {@code Character::isDigit} - a dash (-), dot (.), underscore (_) or tilde (~) - part of any of
 * the ranges of character codes specified when creating the {@code IriSafeMaker} through its
 * constructor {@link #IriSafeMaker(List, Form, boolean)}. When creating an {@code IriSafeMaker}
 * through {@link #create}, these ranges correspond to the {@code ucschar} production rule from
 * RFC-3987, as specified by <a href=
 * "https://www.w3.org/TR/r2rml/#from-template">https://www.w3.org/TR/r2rml/#from-template</a>.
 */
public class IriSafeMaker implements UnaryOperator<String> {

  public static String makeSafe(String input, Form normalizationForm, boolean upperCaseHex) {
    return create(normalizationForm, upperCaseHex).apply(input);
  }

  static class Range {

    final int from;

    final int to;

    Range(int from, int to) {
      this.from = from;
      this.to = to;
    }

    boolean includes(int value) {
      return value >= from && value <= to;
    }
  }

  public static IriSafeMaker create(Form normalizationForm, boolean upperCaseHex) {

    /*
     * percent-encode any char not in the 'iunreserved' production rule: iunreserved = ALPHA / DIGIT /
     * "-" / "." / "_" / "~" / ucschar ucschar = %xA0-D7FF / %xF900-FDCF / %xFDF0-FFEF / %x10000-1FFFD /
     * %x20000-2FFFD / %x30000-3FFFD / %x40000-4FFFD / %x50000-5FFFD / %x60000-6FFFD / %x70000-7FFFD /
     * %x80000-8FFFD / %x90000-9FFFD / %xA0000-AFFFD / %xB0000-BFFFD / %xC0000-CFFFD / %xD0000-DFFFD /
     * %xE1000-EFFFD
     */

    String rangesStr = "%xA0-D7FF / %xF900-FDCF / %xFDF0-FFEF " + "/ %x10000-1FFFD / %x20000-2FFFD / %x30000-3FFFD "
        + "/ %x40000-4FFFD / %x50000-5FFFD / %x60000-6FFFD " + "/ %x70000-7FFFD / %x80000-8FFFD / %x90000-9FFFD "
        + "/ %xA0000-AFFFD / %xB0000-BFFFD / %xC0000-CFFFD " + "/ %xD0000-DFFFD / %xE1000-EFFFD";

    List<Range> ranges = Arrays.asList(rangesStr.split("/"))
        .stream()
        .map(p -> {
          String[] range = p.trim()
              .substring(2)
              .split("-");
          return new Range(Integer.parseInt(range[0], 16), Integer.parseInt(range[1], 16));
        })
        .collect(Collectors.toList());

    return new IriSafeMaker(ranges, normalizationForm, upperCaseHex);
  }

  public static IriSafeMaker create() {
    return create(Form.NFC, true);
  }

  private List<Range> ranges;

  private Form normalizationForm;

  private boolean upperCaseHex;

  public IriSafeMaker(List<Range> ranges, Form normalizationForm, boolean upperCaseHex) {
    this.ranges = ranges;
    this.normalizationForm = normalizationForm;
    this.upperCaseHex = upperCaseHex;
  }

  @Override
  public String apply(String s) {
    StringBuilder result = new StringBuilder();

    s = Normalizer.normalize(s, normalizationForm);
    s.codePoints()
        .flatMap(c -> {

          if (Character.isAlphabetic(c) || Character.isDigit(c) || c == '-' || c == '.' || c == '_' || c == '~'
              || ranges.stream()
                  .anyMatch(r -> r.includes(c))) {
            return IntStream.of(c);
          }

          String hex = Integer.toHexString(c);

          hex = upperCaseHex ? hex.toUpperCase() : hex;

          // percent-encode
          return ("%" + hex).codePoints();
        })
        .forEach(c -> result.append((char) c));
    return result.toString();
  }

}
