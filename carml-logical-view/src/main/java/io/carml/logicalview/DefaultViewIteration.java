package io.carml.logicalview;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ReferenceFormulation;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;

class DefaultViewIteration implements ViewIteration, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final int index;

    private final transient Map<String, Object> values;

    private final transient Map<String, ReferenceFormulation> referenceFormulations;

    private final transient Map<String, IRI> naturalDatatypes;

    /**
     * Source evaluation is intentionally transient — it holds a reference to runtime resolver state
     * (record evaluators) that cannot meaningfully be reconstituted after deserialization. Join
     * parent iterations are constructed with a {@code null} sourceEvaluation by the evaluator's
     * default {@link EvaluationContext}, so this is a no-op for the spill-to-disk path.
     */
    private final transient ExpressionEvaluation sourceEvaluation;

    DefaultViewIteration(
            int index,
            Map<String, Object> values,
            Map<String, ReferenceFormulation> referenceFormulations,
            Map<String, IRI> naturalDatatypes) {
        this(index, values, referenceFormulations, naturalDatatypes, null);
    }

    DefaultViewIteration(
            int index,
            Map<String, Object> values,
            Map<String, ReferenceFormulation> referenceFormulations,
            Map<String, IRI> naturalDatatypes,
            ExpressionEvaluation sourceEvaluation) {
        this.index = index;
        // Use Collections.unmodifiableMap to support null values (left join no-match fields)
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        this.referenceFormulations = Map.copyOf(referenceFormulations);
        this.naturalDatatypes = Map.copyOf(naturalDatatypes);
        this.sourceEvaluation = sourceEvaluation;
    }

    @Override
    public Optional<Object> getValue(String key) {
        return Optional.ofNullable(values.get(key));
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public Set<String> getKeys() {
        return values.keySet();
    }

    @Override
    public Optional<ReferenceFormulation> getFieldReferenceFormulation(String key) {
        return Optional.ofNullable(referenceFormulations.get(key));
    }

    @Override
    public Optional<IRI> getNaturalDatatype(String key) {
        return Optional.ofNullable(naturalDatatypes.get(key));
    }

    @Override
    public Optional<ExpressionEvaluation> getSourceEvaluation() {
        return Optional.ofNullable(sourceEvaluation);
    }

    @Serial
    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    @Serial
    private void readObject(ObjectInputStream in) throws InvalidObjectException {
        // Forces use of the SerializationProxy via writeReplace; never read the original directly.
        throw new InvalidObjectException("Use SerializationProxy");
    }

    private static final class SerializationProxy implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private final int index;

        // Values are produced by RML resolvers and are always one of String, Boolean, Number,
        // or null — all Serializable. The Map<String, Object> declared type cannot express this
        // statically, but the API contract is enforced by the resolver layer.
        @SuppressWarnings("java:S1948")
        private final LinkedHashMap<String, Object> values;

        // The declared Map<String, String> type (from ReferenceFormulationCodec.toIris) is an
        // interface and not statically Serializable, but the codec always returns a
        // LinkedHashMap instance — Serializable and insertion-order preserving — so the field is
        // safe on the serialization wire.
        @SuppressWarnings("serial")
        private final Map<String, String> referenceFormulationIris;

        private final LinkedHashMap<String, IRI> naturalDatatypes;

        SerializationProxy(DefaultViewIteration source) {
            this.index = source.index;
            this.values = new LinkedHashMap<>(source.values);
            this.referenceFormulationIris = ReferenceFormulationCodec.toIris(source.referenceFormulations);
            this.naturalDatatypes = new LinkedHashMap<>(source.naturalDatatypes);
        }

        @Serial
        private Object readResolve() throws ObjectStreamException {
            return new DefaultViewIteration(
                    index,
                    values,
                    ReferenceFormulationCodec.fromIris(referenceFormulationIris),
                    naturalDatatypes,
                    null);
        }
    }
}
