package com.taxonic.carml.engine.template;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;

public class TemplateParser {

  private String escapableChars;

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

  // TODO: PM: reduce cognitive complexity by splitting up in sub-methods
  public Template parse(String template) {

    MutableBoolean escaping = new MutableBoolean(false);
    MutableObject<Segment> segmentContainer = new MutableObject<Segment>(new Text());

    List<CarmlTemplate.Segment> segments = new ArrayList<>();

    MutableInt nextId = new MutableInt();

    Runnable close = () -> {
      Segment segment = segmentContainer.getValue();
      if (segment == null)
        return;
      String value = segment.value();
      if (value.isEmpty())
        return;
      if (segment instanceof Text)
        segments.add(new CarmlTemplate.Text(value));
      else if (segment instanceof Expression)
        segments.add(new CarmlTemplate.ExpressionSegment(nextId.getAndIncrement(), value));
      // (assuming no other segment types)
    };

    IntStream.range(0, template.length())
        .mapToObj(i -> template.substring(i, i + 1))
        .forEach(c -> {

          Segment segment = segmentContainer.getValue();

          if (escaping.booleanValue()) {
            if (!escapableChars.contains(c))
              throw new RuntimeException(
                  "invalid escape sequence in template [" + template + "] - escaping char [" + c + "]");
            segment.append(c);
            escaping.setFalse();
          }

        else if (c.equals("\\"))
            escaping.setTrue();

          else if (c.equals("{")) {
            if (segment instanceof Expression)
              throw new RuntimeException("encountered unescaped nested { character in template [" + template + "]");
            // (assuming segment is Text)
            close.run();
            segmentContainer.setValue(new Expression());
          }

        else if (c.equals("}")) {
            if (segment instanceof Expression) {
              close.run();
              segmentContainer.setValue(new Text());
            }
            // allow adding } outside expressions to text segment
            else
              segment.append(c);
            // (assuming no other segment types)
          }

        else
            segment.append(c);

        });

    Segment segment = segmentContainer.getValue();
    if (segment instanceof Expression)
      throw new RuntimeException("unclosed expression in template [" + template + "]");
    close.run();

    return CarmlTemplate.build(segments);
  }

}
