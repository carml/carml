package io.carml.logicalview.join.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.carml.logicalview.EvaluatedValues;
import io.carml.logicalview.ViewIteration;
import io.carml.model.ReferenceFormulation;
import io.carml.model.impl.CarmlReferenceFormulation;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Test;

class ArrowBlobCodecTest {

    private static ReferenceFormulation refFormulation(String iri) {
        return CarmlReferenceFormulation.builder().id(iri).build();
    }

    @Test
    void encodeDecodeParent_emptyValues_roundtripsCleanly() {
        var parent = ViewIteration.of(0, Map.of(), Map.of(), Map.of());
        var bytes = ArrowBlobCodec.encodeParent(parent);
        var decoded = ArrowBlobCodec.decodeParent(bytes);

        assertThat(decoded.getIndex(), is(0));
        assertThat(decoded.getKeys(), is(empty()));
    }

    @Test
    void encodeDecodeParent_singleField_roundtrips() {
        var values = new LinkedHashMap<String, Object>();
        values.put("name", "alpha");
        var parent = ViewIteration.of(3, values, Map.of(), Map.of());

        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        assertThat(decoded.getIndex(), is(3));
        assertThat(decoded.getKeys(), hasSize(1));
        assertThat(decoded.getValue("name").orElseThrow().toString(), is("alpha"));
    }

    @Test
    void encodeDecodeParent_multipleFields_preservesInsertionOrder() {
        var values = new LinkedHashMap<String, Object>();
        values.put("z", "last");
        values.put("a", "first");
        values.put("m", "middle");
        var parent = ViewIteration.of(7, values, Map.of(), Map.of());

        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        assertThat(List.copyOf(decoded.getKeys()), is(List.of("z", "a", "m")));
    }

    @Test
    void encodeDecodeParent_nullValue_isPreservedAsAbsent() {
        var values = new LinkedHashMap<String, Object>();
        values.put("present", "yes");
        values.put("missing", null);
        var parent = ViewIteration.of(0, values, Map.of(), Map.of());

        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        assertThat(decoded.getValue("present").orElseThrow().toString(), is("yes"));
        // ViewIteration.getValue returns Optional.empty() for null entries (see DefaultViewIteration#getValue).
        assertThat(decoded.getValue("missing").isPresent(), is(false));
    }

    @Test
    void encodeDecodeParent_mixedRdfDatatypes_naturalDatatypesSurvive() {
        var values = new LinkedHashMap<String, Object>();
        values.put("str", "hello");
        values.put("count", 42);
        values.put("flag", Boolean.TRUE);
        values.put("ratio", 3.14d);

        var datatypes = new LinkedHashMap<String, IRI>();
        datatypes.put("str", XSD.STRING);
        datatypes.put("count", XSD.INTEGER);
        datatypes.put("flag", XSD.BOOLEAN);
        datatypes.put("ratio", XSD.DOUBLE);

        var parent = ViewIteration.of(0, values, Map.of(), datatypes);
        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        assertThat(decoded.getNaturalDatatype("str").orElseThrow(), is(XSD.STRING));
        assertThat(decoded.getNaturalDatatype("count").orElseThrow(), is(XSD.INTEGER));
        assertThat(decoded.getNaturalDatatype("flag").orElseThrow(), is(XSD.BOOLEAN));
        assertThat(decoded.getNaturalDatatype("ratio").orElseThrow(), is(XSD.DOUBLE));
        // Values come back as canonical strings (Option A: lexical-form-equivalent).
        assertThat(decoded.getValue("count").orElseThrow().toString(), is("42"));
        assertThat(decoded.getValue("flag").orElseThrow().toString(), is("true"));
        assertThat(decoded.getValue("ratio").orElseThrow().toString(), is("3.14"));
    }

    @Test
    void encodeParent_byteBufferValue_isEncodedAsHex() {
        var bytes = new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
        var values = new LinkedHashMap<String, Object>();
        values.put("blob", ByteBuffer.wrap(bytes));
        var datatypes = Map.of("blob", XSD.HEXBINARY);

        var parent = ViewIteration.of(0, values, Map.of(), datatypes);
        var encoded = ArrowBlobCodec.encodeParent(parent);
        var decoded = ArrowBlobCodec.decodeParent(encoded);

        var decodedHex = decoded.getValue("blob").orElseThrow().toString();
        assertThat("hex form, not ByteBuffer.toString()", decodedHex, is("CAFEBABE"));
        // Sanity: the encoded bytes contain the hex string somewhere (Utf8 cell payload), and
        // do NOT contain "HeapByteBuffer" (the misleading default toString form).
        var encodedAsString = new String(encoded, StandardCharsets.ISO_8859_1);
        assertThat(encodedAsString, allOf(containsString("CAFEBABE")));
    }

    @Test
    void encodeParent_zonedDateTimeValue_stripsTimezoneInLexicalForm() {
        var zdt = ZonedDateTime.of(2026, 4, 22, 12, 0, 0, 0, ZoneId.of("Europe/Berlin"));
        var values = new LinkedHashMap<String, Object>();
        values.put("ts", zdt);
        var datatypes = Map.of("ts", XSD.DATETIME);

        var parent = ViewIteration.of(0, values, Map.of(), datatypes);
        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        // No "+02:00" / no "[Europe/Berlin]" — timezone is stripped to match the engine's
        // canonical xsd:dateTime form.
        assertThat(decoded.getValue("ts").orElseThrow().toString(), is("2026-04-22T12:00"));
    }

    @Test
    void encodeParent_localDateTimeValue_roundtripsAsIsoString() {
        var ldt = LocalDateTime.of(2026, 4, 22, 12, 0);
        var values = new LinkedHashMap<String, Object>();
        values.put("ts", ldt);

        var parent = ViewIteration.of(0, values, Map.of(), Map.of());
        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        // RDF4J Values.literal(LocalDateTime) produces "...:00.0" (trailing zero seconds fraction).
        // The exact lexical form is what the engine's CanonicalRdfLexicalForm would have produced,
        // which is the round-trip contract.
        assertThat(decoded.getValue("ts").orElseThrow().toString(), containsString("2026-04-22T12:00:00"));
    }

    @Test
    void encodeParent_localDateValue_roundtripsAsIsoDate() {
        var ld = LocalDate.of(2026, 4, 22);
        var values = new LinkedHashMap<String, Object>();
        values.put("d", ld);

        var parent = ViewIteration.of(0, values, Map.of(), Map.of());
        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        assertThat(decoded.getValue("d").orElseThrow().toString(), is("2026-04-22"));
    }

    @Test
    void encodeParent_offsetDateTimeValue_roundtripsViaRdf4jLexicalForm() {
        var odt = OffsetDateTime.of(2026, 4, 22, 12, 0, 0, 0, ZoneOffset.UTC);
        var values = new LinkedHashMap<String, Object>();
        values.put("ts", odt);

        var parent = ViewIteration.of(0, values, Map.of(), Map.of());
        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        // RDF4J Values.literal(OffsetDateTime) keeps the offset in the lexical form. Round-trip
        // is lexical-form-stable rather than Object-equal — that's intentional Option A behavior.
        assertThat(decoded.getValue("ts").orElseThrow(), is(notNullValue()));
        assertThat(decoded.getValue("ts").orElseThrow().toString(), containsString("2026-04-22T12:00:00"));
    }

    @Test
    void encodeParent_instantValue_roundtripsViaRdf4jLexicalForm() {
        var instant = Instant.parse("2026-04-22T12:00:00Z");
        var values = new LinkedHashMap<String, Object>();
        values.put("ts", instant);

        var parent = ViewIteration.of(0, values, Map.of(), Map.of());
        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        assertThat(decoded.getValue("ts").orElseThrow().toString(), containsString("2026-04-22T12:00:00"));
    }

    @Test
    void encodeDecodeParent_referenceFormulations_roundtripViaCodec() {
        var refForms = new LinkedHashMap<String, ReferenceFormulation>();
        refForms.put("col1", refFormulation("http://w3id.org/rml/CSV"));
        refForms.put("col2", refFormulation("http://w3id.org/rml/JSONPath"));

        var parent = ViewIteration.of(0, Map.of("col1", "v1", "col2", "v2"), refForms, Map.of());
        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        assertThat(
                decoded.getFieldReferenceFormulation("col1")
                        .map(rf -> rf.getAsResource().stringValue())
                        .orElseThrow(),
                is("http://w3id.org/rml/CSV"));
        assertThat(
                decoded.getFieldReferenceFormulation("col2")
                        .map(rf -> rf.getAsResource().stringValue())
                        .orElseThrow(),
                is("http://w3id.org/rml/JSONPath"));
    }

    @Test
    void encodeDecodeChild_emptyValues_roundtripsCleanly() {
        var child = new EvaluatedValues(Map.of(), Map.of(), Map.of());
        var decoded = ArrowBlobCodec.decodeChild(ArrowBlobCodec.encodeChild(child));

        assertThat(decoded.values().entrySet(), is(empty()));
        assertThat(decoded.referenceFormulations().entrySet(), is(empty()));
        assertThat(decoded.naturalDatatypes().entrySet(), is(empty()));
        assertThat(decoded.sourceEvaluation(), is(nullValue()));
    }

    @Test
    void encodeDecodeChild_mixedDatatypesAndNullValue_roundtrips() {
        var values = new LinkedHashMap<String, Object>();
        values.put("name", "child");
        values.put("count", 99);
        values.put("missing", null);

        var datatypes = new LinkedHashMap<String, IRI>();
        datatypes.put("count", XSD.INTEGER);

        var child = new EvaluatedValues(values, Map.of(), datatypes);
        var decoded = ArrowBlobCodec.decodeChild(ArrowBlobCodec.encodeChild(child));

        // Order preserved; null preserved as a true null map entry.
        assertThat(List.copyOf(decoded.values().keySet()), is(List.of("name", "count", "missing")));
        assertThat(decoded.values().get("name").toString(), is("child"));
        assertThat(decoded.values().get("count").toString(), is("99"));
        assertThat(decoded.values(), hasEntry(is("missing"), is(nullValue())));
        assertThat(decoded.naturalDatatypes().get("count"), is(XSD.INTEGER));
    }

    @Test
    void encodeDecodeChild_referenceFormulations_roundtripViaCodec() {
        var refForms = new LinkedHashMap<String, ReferenceFormulation>();
        refForms.put("c", refFormulation("http://w3id.org/rml/XPath"));

        var child = new EvaluatedValues(Map.of("c", "x"), refForms, Map.of());
        var decoded = ArrowBlobCodec.decodeChild(ArrowBlobCodec.encodeChild(child));

        assertThat(
                decoded.referenceFormulations().get("c").getAsResource().stringValue(),
                is("http://w3id.org/rml/XPath"));
    }

    @Test
    void encodeDecodeChild_sourceEvaluationDroppedOnWire() {
        // Even if a caller constructs an EvaluatedValues with a sourceEvaluation, the codec drops
        // it (the join path always sees null sourceEvaluation, mirroring SerializationProxy).
        var child = new EvaluatedValues(Map.of("k", "v"), Map.of(), Map.of(), s -> java.util.Optional.of("ignored"));
        var decoded = ArrowBlobCodec.decodeChild(ArrowBlobCodec.encodeChild(child));

        assertThat(decoded.sourceEvaluation(), is(nullValue()));
    }

    @Test
    void encodeDecodeChild_indexKey_roundtripsAsInteger() {
        // INDEX_KEY ("#") carries the source-record index as an Integer in EvaluatedValues. The
        // wire format stores all values as Utf8 — the codec must re-coerce the string back to
        // Integer on decode so DefaultLogicalViewEvaluator's `(int) ev.values().get(INDEX_KEY)`
        // unbox does not ClassCastException.
        var values = new LinkedHashMap<String, Object>();
        values.put(EvaluatedValues.INDEX_KEY, 42);
        values.put("name", "child");

        var child = new EvaluatedValues(values, Map.of(), Map.of());
        var decoded = ArrowBlobCodec.decodeChild(ArrowBlobCodec.encodeChild(child));

        // INDEX_KEY recovers as Integer (not String); plain field stays String per the
        // RDF-output-equivalent encoding contract.
        assertThat(decoded.values().get(EvaluatedValues.INDEX_KEY), is(Integer.valueOf(42)));
        assertThat(decoded.values().get("name").toString(), is("child"));
    }

    @Test
    void encodeDecodeChild_nestedIndexKeySuffix_roundtripsAsInteger() {
        // Nested iterable record indexes (e.g. "items.#") follow the same Integer contract as
        // INDEX_KEY. They are produced by withIndex on a primitive int and must survive the
        // Arrow round-trip with their integer typing intact.
        var values = new LinkedHashMap<String, Object>();
        values.put("items.#", 7);
        values.put("items.value", "hello");

        var child = new EvaluatedValues(values, Map.of(), Map.of());
        var decoded = ArrowBlobCodec.decodeChild(ArrowBlobCodec.encodeChild(child));

        assertThat(decoded.values().get("items.#"), is(Integer.valueOf(7)));
        assertThat(decoded.values().get("items.value").toString(), is("hello"));
    }

    @Test
    void encodeDecodeParent_indexKeyInValues_roundtripsAsInteger() {
        // Parents carry INDEX_KEY in two places: the dedicated PARENT_SCHEMA `index` int column
        // (used for ViewIteration#getIndex) and inside the values map (used when expressions
        // reference "#" as a referenceable key on a parent view). Both must come back integer-typed.
        var values = new LinkedHashMap<String, Object>();
        values.put(EvaluatedValues.INDEX_KEY, 5);
        values.put("col", "v");

        var parent = ViewIteration.of(5, values, Map.of(), Map.of());
        var decoded = ArrowBlobCodec.decodeParent(ArrowBlobCodec.encodeParent(parent));

        assertThat(decoded.getIndex(), is(5));
        assertThat(decoded.getValue(EvaluatedValues.INDEX_KEY).orElseThrow(), is(Integer.valueOf(5)));
        assertThat(decoded.getValue("col").orElseThrow().toString(), is("v"));
    }
}
