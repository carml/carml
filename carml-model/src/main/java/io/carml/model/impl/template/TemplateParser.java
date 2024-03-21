package io.carml.model.impl.template;

import io.carml.model.Template;
import io.carml.model.impl.CarmlTemplate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;

public class TemplateParser {

    private static final TemplateParser DEFAULT = TemplateParser.of();

    private final String escapableChars;

    private TemplateParser(String escapableChars) {
        this.escapableChars = escapableChars;
    }

    public static TemplateParser getInstance() {
        return DEFAULT;
    }

    public static TemplateParser of() {
        return new TemplateParser("{}\\");
    }

    private abstract static class Segment {

        private final StringBuilder stringBuilder = new StringBuilder();

        void append(String character) {
            stringBuilder.append(character);
        }

        String value() {
            return stringBuilder.toString();
        }
    }

    private static class TextSegment extends Segment {}

    private static class ExpressionSegment extends Segment {}

    public Template parse(String template) {
        MutableBoolean escaping = new MutableBoolean(false);
        MutableObject<Segment> segmentContainer = new MutableObject<>(new TextSegment());

        List<Template.Segment> segments = new ArrayList<>();

        Runnable storeSegment = getStoreSegmentRunnable(segmentContainer, segments);

        IntStream.range(0, template.length())
                .mapToObj(i -> template.substring(i, i + 1))
                .forEach(character ->
                        parseCharToSegments(template, character, segmentContainer, escaping, storeSegment));

        Segment segment = segmentContainer.getValue();
        if (segment instanceof ExpressionSegment) {
            throw new TemplateException(String.format("unclosed expression in template [%s]", template));
        }
        storeSegment.run();

        return CarmlTemplate.of(segments);
    }

    private Runnable getStoreSegmentRunnable(MutableObject<Segment> segmentContainer, List<Template.Segment> segments) {
        MutableInt nextId = new MutableInt();

        return () -> {
            Segment segment = segmentContainer.getValue();
            if (segment == null) {
                return;
            }

            String value = segment.value();
            if (value.isEmpty()) {
                return;
            }

            if (segment instanceof TextSegment) {
                segments.add(new CarmlTemplate.TextSegment(value));
            } else if (segment instanceof ExpressionSegment) {
                segments.add(new CarmlTemplate.ExpressionSegment(nextId.getAndIncrement(), value));
            }
            // (assuming no other segment types)
        };
    }

    private void parseCharToSegments(
            String template,
            String character,
            MutableObject<Segment> segmentContainer,
            MutableBoolean escaping,
            Runnable storeSegment) {
        Segment segment = segmentContainer.getValue();

        if (escaping.booleanValue()) {
            if (!escapableChars.contains(character)) {
                throw new TemplateException(String.format(
                        "invalid escape sequence in template [%s] - escaping char [%s]", template, character));
            }
            segment.append(character);
            escaping.setFalse();
        } else if (character.equals("\\")) {
            escaping.setTrue();
        } else if (character.equals("{")) {
            if (segment instanceof ExpressionSegment) {
                throw new TemplateException(
                        String.format("encountered unescaped nested { character in template [%s]", template));
            }
            // (assuming segment is Text)
            storeSegment.run();
            segmentContainer.setValue(new ExpressionSegment());
        } else if (character.equals("}")) {
            if (segment instanceof ExpressionSegment) {
                storeSegment.run();
                segmentContainer.setValue(new TextSegment());
            } else { // allow adding } outside expressions to text segment
                segment.append(character);
            }
            // (assuming no other segment types)
        } else {
            segment.append(character);
        }
    }
}
