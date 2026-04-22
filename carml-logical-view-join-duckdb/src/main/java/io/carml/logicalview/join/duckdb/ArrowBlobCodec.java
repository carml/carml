package io.carml.logicalview.join.duckdb;

import io.carml.logicalview.EvaluatedValues;
import io.carml.logicalview.ReferenceFormulationCodec;
import io.carml.logicalview.ViewIteration;
import io.carml.model.ReferenceFormulation;
import jakarta.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.Values;

/**
 * Encodes / decodes {@link ViewIteration} and {@link EvaluatedValues} as Arrow IPC streams for use
 * as DuckDB BLOB column payloads in {@link DuckDbJoinExecutor}.
 *
 * <p>The wire schema stores all values as Utf8 with a parallel {@code natural_datatypes} map of IRI
 * strings; this is RDF-output-equivalent to the previous Java-serialized {@code Map<String,
 * Object>} because downstream lexical-form generation (see CARML's {@code CanonicalRdfLexicalForm})
 * is datatype-driven, not Java-runtime-type-driven. Two encode-time conversions preserve canonical
 * forms for non-string source types:
 *
 * <ul>
 *   <li>{@link ByteBuffer} values are hex-printed (matches {@code xsd:hexBinary} canonical form).
 *   <li>{@link ZonedDateTime} values have the timezone stripped via {@code toLocalDateTime()} so
 *       that {@code xsd:dateTime} canonical form does not carry an offset that the engine would
 *       otherwise drop.
 *   <li>Other {@link TemporalAccessor} values are normalized via {@link Values#literal} to match
 *       the engine's canonical lexical form.
 * </ul>
 *
 * <p>{@link EvaluatedValues#sourceEvaluation()} is dropped on the wire — it is always {@code null}
 * in the join path, mirroring the existing {@code SerializationProxy} behavior.
 *
 * <p>Each encode / decode call allocates and closes its own {@link RootAllocator}; per-executor
 * or shared allocators are an optimization for a follow-up if benchmarks identify allocator setup
 * as hot. Schema instances are cached as {@code static final} fields since they are immutable.
 */
final class ArrowBlobCodec {

    private static final String INDEX_FIELD = "index";

    private static final String VALUES_FIELD = "values";

    private static final String NATURAL_DATATYPES_FIELD = "natural_datatypes";

    private static final String REFERENCE_FORMULATIONS_FIELD = "reference_formulations";

    private static final FieldType NULLABLE_UTF8 = FieldType.nullable(new ArrowType.Utf8());

    private static final FieldType NON_NULLABLE_UTF8 = FieldType.notNullable(new ArrowType.Utf8());

    private static final Schema PARENT_SCHEMA = new Schema(List.of(
            new Field(INDEX_FIELD, FieldType.notNullable(new ArrowType.Int(32, true)), null),
            mapField(VALUES_FIELD, true),
            mapField(NATURAL_DATATYPES_FIELD, false),
            mapField(REFERENCE_FORMULATIONS_FIELD, false)));

    private static final Schema CHILD_SCHEMA = new Schema(List.of(
            mapField(VALUES_FIELD, true),
            mapField(NATURAL_DATATYPES_FIELD, false),
            mapField(REFERENCE_FORMULATIONS_FIELD, false)));

    private ArrowBlobCodec() {}

    /**
     * Encodes a parent {@link ViewIteration} as a single-row Arrow IPC byte stream.
     */
    static byte[] encodeParent(ViewIteration parent) {
        try (var allocator = new RootAllocator();
                var root = VectorSchemaRoot.create(PARENT_SCHEMA, allocator)) {
            var indexVector = (IntVector) root.getVector(INDEX_FIELD);
            indexVector.setSafe(0, parent.getIndex());
            writeValueMap(root, collectValues(parent));
            writeIriMap(root, collectNaturalDatatypes(parent));
            writeStringMap(
                    root,
                    REFERENCE_FORMULATIONS_FIELD,
                    ReferenceFormulationCodec.toIris(collectReferenceFormulations(parent)));
            root.setRowCount(1);
            return writeIpc(root);
        }
    }

    /**
     * Encodes a child {@link EvaluatedValues} row as an Arrow IPC byte stream.
     */
    static byte[] encodeChild(EvaluatedValues child) {
        try (var allocator = new RootAllocator();
                var root = VectorSchemaRoot.create(CHILD_SCHEMA, allocator)) {
            writeValueMap(root, child.values());
            writeIriMap(root, child.naturalDatatypes());
            writeStringMap(
                    root,
                    REFERENCE_FORMULATIONS_FIELD,
                    ReferenceFormulationCodec.toIris(child.referenceFormulations()));
            root.setRowCount(1);
            return writeIpc(root);
        }
    }

    /**
     * Decodes a parent BLOB back into a {@link ViewIteration}.
     */
    static ViewIteration decodeParent(byte[] bytes) {
        try (var allocator = new RootAllocator();
                var reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            if (!reader.loadNextBatch()) {
                throw new IllegalStateException("Empty Arrow stream when decoding parent ViewIteration");
            }
            var root = reader.getVectorSchemaRoot();
            var indexVector = (IntVector) root.getVector(INDEX_FIELD);
            var index = indexVector.get(0);
            var values = readValueMap(root);
            var naturalDatatypes = readIriMap(root);
            var referenceFormulations = ReferenceFormulationCodec.fromIris(readReferenceFormulationIris(root));
            return ViewIteration.of(index, values, referenceFormulations, naturalDatatypes);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decode parent ViewIteration from Arrow IPC", e);
        }
    }

    /**
     * Decodes a child BLOB back into an {@link EvaluatedValues}.
     */
    static EvaluatedValues decodeChild(byte[] bytes) {
        try (var allocator = new RootAllocator();
                var reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            if (!reader.loadNextBatch()) {
                throw new IllegalStateException("Empty Arrow stream when decoding child EvaluatedValues");
            }
            var root = reader.getVectorSchemaRoot();
            var values = readValueMap(root);
            var naturalDatatypes = readIriMap(root);
            var referenceFormulations = ReferenceFormulationCodec.fromIris(readReferenceFormulationIris(root));
            return new EvaluatedValues(values, referenceFormulations, naturalDatatypes);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decode child EvaluatedValues from Arrow IPC", e);
        }
    }

    private static Map<String, Object> collectValues(ViewIteration parent) {
        var values = new LinkedHashMap<String, Object>();
        for (var key : parent.getKeys()) {
            values.put(key, parent.getValue(key).orElse(null));
        }
        return values;
    }

    private static Map<String, IRI> collectNaturalDatatypes(ViewIteration parent) {
        var datatypes = new LinkedHashMap<String, IRI>();
        for (var key : parent.getKeys()) {
            parent.getNaturalDatatype(key).ifPresent(iri -> datatypes.put(key, iri));
        }
        return datatypes;
    }

    private static Map<String, ReferenceFormulation> collectReferenceFormulations(ViewIteration parent) {
        var formulations = new LinkedHashMap<String, ReferenceFormulation>();
        for (var key : parent.getKeys()) {
            parent.getFieldReferenceFormulation(key).ifPresent(rf -> formulations.put(key, rf));
        }
        return formulations;
    }

    private static byte[] writeIpc(VectorSchemaRoot root) {
        var baos = new ByteArrayOutputStream();
        try (var writer = new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), baos)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write Arrow IPC stream", e);
        }
        return baos.toByteArray();
    }

    private static void writeValueMap(VectorSchemaRoot root, Map<String, Object> entries) {
        var mapVector = (MapVector) root.getVector(VALUES_FIELD);
        var writer = mapVector.getWriter();
        writer.setPosition(0);
        writer.startMap();
        if (entries != null) {
            for (var entry : entries.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                writer.startEntry();
                writer.key().varChar().writeVarChar(entry.getKey());
                var canonical = canonicalString(entry.getValue());
                if (canonical == null) {
                    // Marks the value child as null while keeping the entry (with non-null key) live.
                    writer.value().varChar().writeNull();
                } else {
                    writer.value().varChar().writeVarChar(canonical);
                }
                writer.endEntry();
            }
        }
        writer.endMap();
        mapVector.setValueCount(1);
    }

    private static void writeIriMap(VectorSchemaRoot root, Map<String, IRI> entries) {
        writeStringMap(root, NATURAL_DATATYPES_FIELD, toIriStringMap(entries));
    }

    private static Map<String, String> toIriStringMap(Map<String, IRI> entries) {
        if (entries == null || entries.isEmpty()) {
            return Map.of();
        }
        var result = new LinkedHashMap<String, String>(entries.size());
        for (var entry : entries.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            result.put(entry.getKey(), entry.getValue().stringValue());
        }
        return result;
    }

    private static void writeStringMap(VectorSchemaRoot root, String fieldName, Map<String, String> entries) {
        var mapVector = (MapVector) root.getVector(fieldName);
        var writer = mapVector.getWriter();
        writer.setPosition(0);
        writer.startMap();
        if (entries != null) {
            for (var entry : entries.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }
                writeStringEntry(writer, entry.getKey(), entry.getValue());
            }
        }
        writer.endMap();
        mapVector.setValueCount(1);
    }

    private static void writeStringEntry(UnionMapWriter writer, String key, String value) {
        writer.startEntry();
        writer.key().varChar().writeVarChar(key);
        writer.value().varChar().writeVarChar(value);
        writer.endEntry();
    }

    private static Map<String, Object> readValueMap(VectorSchemaRoot root) {
        return readMap(root, VALUES_FIELD, Function.identity());
    }

    private static Map<String, IRI> readIriMap(VectorSchemaRoot root) {
        return readMap(root, NATURAL_DATATYPES_FIELD, raw -> Values.iri((String) raw));
    }

    private static Map<String, String> readReferenceFormulationIris(VectorSchemaRoot root) {
        return readMap(root, REFERENCE_FORMULATIONS_FIELD, String.class::cast);
    }

    private static <V> LinkedHashMap<String, V> readMap(
            VectorSchemaRoot root, String fieldName, Function<Object, V> valueMapper) {
        var mapVector = (MapVector) root.getVector(fieldName);
        var result = new LinkedHashMap<String, V>();
        if (mapVector.isNull(0)) {
            return result;
        }
        var entries = (StructVector) mapVector.getDataVector();
        // Each Int32 offset entry is 4 bytes wide; offsets[0]..offsets[1] gives this row's slice.
        var start = mapVector.getOffsetBuffer().getInt(0L);
        var end = mapVector.getOffsetBuffer().getInt(4L);
        var keyVector = entries.getChild(MapVector.KEY_NAME);
        var valueVector = entries.getChild(MapVector.VALUE_NAME);
        for (var i = start; i < end; i++) {
            var rawKey = keyVector.getObject(i);
            if (rawKey == null) {
                continue;
            }
            var key = rawKey instanceof Text text ? text.toString() : rawKey.toString();
            if (valueVector.isNull(i)) {
                result.put(key, null);
            } else {
                var rawValue = valueVector.getObject(i);
                var stringValue = rawValue instanceof Text text ? text.toString() : rawValue;
                result.put(key, valueMapper.apply(stringValue));
            }
        }
        return result;
    }

    private static Field mapField(String name, boolean valuesNullable) {
        var keyField = new Field(MapVector.KEY_NAME, NON_NULLABLE_UTF8, null);
        var valueField = new Field(MapVector.VALUE_NAME, valuesNullable ? NULLABLE_UTF8 : NON_NULLABLE_UTF8, null);
        var entryField = new Field(
                MapVector.DATA_VECTOR_NAME,
                FieldType.notNullable(new ArrowType.Struct()),
                List.of(keyField, valueField));
        return new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of(entryField));
    }

    /**
     * Returns the canonical string form for a value to be embedded in an Arrow Utf8 cell. Mirrors
     * the type-aware lexical-form choices in CARML's {@code CanonicalRdfLexicalForm} so the codec
     * stores forms equivalent to what the engine would have produced from the original
     * {@code Object} value.
     */
    private static String canonicalString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof ByteBuffer byteBuffer) {
            // Match xsd:hexBinary canonical form. RML resolvers produce array-backed buffers (the
            // ByteBuffer wraps a byte[] holding the source bytes).
            return DatatypeConverter.printHexBinary(byteBuffer.array());
        }
        if (value instanceof ZonedDateTime zdt) {
            // Strip timezone for xsd:dateTime canonical form — the engine does the same in
            // CanonicalRdfLexicalForm so the round-trip via ZonedDateTime.toString() (which keeps
            // the offset) would otherwise diverge.
            return zdt.toLocalDateTime().toString();
        }
        if (value instanceof TemporalAccessor temporalAccessor) {
            // Defer to RDF4J's Values.literal() for canonical lexical form (matches what the
            // engine's CanonicalRdfLexicalForm produces via Values.literal(...).stringValue()).
            return Values.literal(temporalAccessor).stringValue();
        }
        return value.toString();
    }
}
