package io.carml.util;

import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import lombok.NonNull;

/**
 * Creates IRI-safe values, by percent-encoding any characters in a {@code String} that are NOT one
 * of the following: - {@code alpha} or {@code digit} as per
 * <a href="https://www.rfc-editor.org/rfc/rfc5234">RFC-5234</a>, a dash (-), dot (.), underscore
 * (_) or tilde (~), or {@code ucschar} as per
 * <a href="https://www.rfc-editor.org/rfc/rfc3987#section-2.2">RFC-3987</a>}, as specified by
 * <a href=
 * "https://www.w3.org/TR/r2rml/#from-template">https://www.w3.org/TR/r2rml/#from-template</a>.
 */
public class IriSafeMaker implements UnaryOperator<String> {

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

  public static IriSafeMaker create() {
    return create(Form.NFC, true);
  }

  public static IriSafeMaker create(@NonNull Form normalizationForm, boolean upperCaseHex) {

    /*
     * percent-encode any char not in the 'iunreserved' production rule: iunreserved = ALPHA / DIGIT /
     * "-" / "." / "_" / "~" / ucschar ucschar = %xA0-D7FF / %xF900-FDCF / %xFDF0-FFEF / %x10000-1FFFD /
     * %x20000-2FFFD / %x30000-3FFFD / %x40000-4FFFD / %x50000-5FFFD / %x60000-6FFFD / %x70000-7FFFD /
     * %x80000-8FFFD / %x90000-9FFFD / %xA0000-AFFFD / %xB0000-BFFFD / %xC0000-CFFFD / %xD0000-DFFFD /
     * %xE1000-EFFFD
     */

    var alphaString = "%x41-5A / %x61-7A";
    var digitString = "%x30-39";
    var ucscharString = "%xA0-D7FF / %xF900-FDCF / %xFDF0-FFEF " //
        + "/ %x10000-1FFFD / %x20000-2FFFD / %x30000-3FFFD " //
        + "/ %x40000-4FFFD / %x50000-5FFFD / %x60000-6FFFD " //
        + "/ %x70000-7FFFD / %x80000-8FFFD / %x90000-9FFFD " //
        + "/ %xA0000-AFFFD / %xB0000-BFFFD / %xC0000-CFFFD " //
        + "/ %xD0000-DFFFD / %xE1000-EFFFD";

    var alpha = parseAbnfRangeString(alphaString);
    var digit = parseAbnfRangeString(digitString);
    var ucschar = parseAbnfRangeString(ucscharString);

    Predicate<Integer> isIunreserved =
        codePoint -> withinRange(codePoint, alpha) || withinRange(codePoint, digit) || codePoint == '-'
            || codePoint == '.' || codePoint == '_' || codePoint == '~' || withinRange(codePoint, ucschar);

    return new IriSafeMaker(isIunreserved, normalizationForm, upperCaseHex);
  }

  private static List<Range> parseAbnfRangeString(String rangeString) {
    return Arrays.stream(rangeString.split("/"))
        .map(rangeStringPart -> {
          String[] range = rangeStringPart.trim()
              .substring(2)
              .split("-");
          return new Range(Integer.parseInt(range[0], 16), Integer.parseInt(range[1], 16));
        })
        .toList();
  }

  private static boolean withinRange(int codePoint, List<Range> ranges) {
    return ranges.stream()
        .anyMatch(range -> range.includes(codePoint));
  }

  private final Predicate<Integer> inIunreserved;

  private final Form normalizationForm;

  private final boolean upperCaseHex;

  private IriSafeMaker(Predicate<Integer> inIunreserved, Form normalizationForm, boolean upperCaseHex) {
    this.inIunreserved = inIunreserved;
    this.normalizationForm = normalizationForm;
    this.upperCaseHex = upperCaseHex;
  }

  @Override
  public String apply(String iriString) {
    var result = new StringBuilder();

    iriString = Normalizer.normalize(iriString, normalizationForm);
    iriString.codePoints()
        .flatMap(this::percentEncode)
        .forEach(c -> result.append((char) c));
    return result.toString();
  }

  private IntStream percentEncode(int codePoint) {

    if (inIunreserved.test(codePoint)) {
      return IntStream.of(codePoint);
    }

    var percentEncoded = upperCaseHex ? String.format("%%%02X", codePoint) : String.format("%%%02x", codePoint);

    return percentEncoded.codePoints();
  }
}
