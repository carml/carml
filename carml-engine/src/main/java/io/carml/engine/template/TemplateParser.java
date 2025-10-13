package io.carml.engine.template;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;

public class TemplateParser {

  private final String escapableChars;

  private TemplateParser(String escapableChars) {
    this.escapableChars = escapableChars;
  }

  public static TemplateParser build() {
    return new TemplateParser("{}\\");
  }

  private abstract static class Segment {

    StringBuilder str = new StringBuilder();

    void append(String c) {
      str.append(c);
    }

    String value() {
      return str.toString();
    }
  }

  private static class Text extends Segment {
  }

  private static class Expression extends Segment {
  }

  public Template parse(String template) {

    MutableBoolean escaping = new MutableBoolean(false);
    MutableObject<Segment> segmentContainer = new MutableObject<>(new Text());

    List<CarmlTemplate.Segment> segments = new ArrayList<>();

    MutableInt nextId = new MutableInt();

    Runnable storeSegment = () -> {
      Segment segment = segmentContainer.get();
      if (segment == null) {
        return;
      }

      String value = segment.value();
      if (value.isEmpty()) {
        return;
      }

      if (segment instanceof Text) {
        segments.add(new CarmlTemplate.Text(value));
      } else if (segment instanceof Expression) {
        segments.add(new CarmlTemplate.ExpressionSegment(nextId.getAndIncrement(), value));
      }
      // (assuming no other segment types)
    };

    IntStream.range(0, template.length())
        .mapToObj(i -> template.substring(i, i + 1))
        .forEach(character -> parseCharToSegments(template, character, segmentContainer, escaping, storeSegment));

    Segment segment = segmentContainer.get();
    if (segment instanceof Expression) {
      throw new TemplateException(String.format("unclosed expression in template [%s]", template));
    }
    storeSegment.run();

    return CarmlTemplate.build(segments);
  }

  private void parseCharToSegments(String template, String character, MutableObject<Segment> segmentContainer,
      MutableBoolean escaping, Runnable storeSegment) {
    Segment segment = segmentContainer.get();

    if (escaping.booleanValue()) {
      if (!escapableChars.contains(character)) {
        throw new TemplateException(
            String.format("invalid escape sequence in template [%s] - escaping char [%s]", template, character));
      }
      segment.append(character);
      escaping.setFalse();
    } else if (character.equals("\\")) {
      escaping.setTrue();
    } else if (character.equals("{")) {
      if (segment instanceof Expression) {
        throw new TemplateException(
            String.format("encountered unescaped nested { character in template [%s]", template));
      }
      // (assuming segment is Text)
      storeSegment.run();
      segmentContainer.setValue(new Expression());
    } else if (character.equals("}")) {
      if (segment instanceof Expression) {
        storeSegment.run();
        segmentContainer.setValue(new Text());
      } else { // allow adding } outside expressions to text segment
        segment.append(character);
      }
      // (assuming no other segment types)
    } else {
      segment.append(character);
    }
  }

}
