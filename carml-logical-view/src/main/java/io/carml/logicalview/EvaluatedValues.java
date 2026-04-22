package io.carml.logicalview;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.LogicalView;
import io.carml.model.ReferenceFormulation;
import java.io.ObjectStreamException;
import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import org.eclipse.rdf4j.model.IRI;

/**
 * One evaluated row produced during {@link LogicalView} evaluation: the field values keyed by
 * absolute name, plus the per-field reference formulations and natural datatypes resolved during
 * evaluation. The optional {@code sourceEvaluation} preserves the source-level expression
 * evaluation for synthetic (implicit) views, enabling expressions not captured as view fields
 * (e.g., gather map references) to be evaluated from the source data.
 *
 * <p>Public so that {@link JoinExecutor} implementations can consume rows without touching the
 * evaluator's internals.
 *
 * <p>Serialization is provided via a {@link SerializationProxy} that converts {@link
 * ReferenceFormulation} instances to IRI strings via {@link ReferenceFormulationCodec} —
 * downstream join consumers only require IRI identity. The {@code sourceEvaluation} is dropped on
 * the wire (set to {@code null} on read) because it references runtime resolver state that
 * cannot meaningfully be reconstituted; the evaluator's default {@link EvaluationContext} already
 * nulls it out for parent iterations during joins.
 */
public record EvaluatedValues(
        Map<String, Object> values,
        Map<String, ReferenceFormulation> referenceFormulations,
        Map<String, IRI> naturalDatatypes,
        ExpressionEvaluation sourceEvaluation)
        implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public EvaluatedValues(
            Map<String, Object> values,
            Map<String, ReferenceFormulation> referenceFormulations,
            Map<String, IRI> naturalDatatypes) {
        this(values, referenceFormulations, naturalDatatypes, null);
    }

    @Serial
    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    // No readObject guard: records cannot define instance methods named readObject, and the field
    // type Map<String, ReferenceFormulation> is not Serializable (CarmlResource is not), so any
    // direct write-without-proxy attempt would fail with NotSerializableException at write time
    // — the SerializationProxy path is the only viable wire format.

    private static final class SerializationProxy implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        // Values are produced by RML resolvers and are always one of String, Boolean, Number,
        // or null — all Serializable. The Map<String, Object> declared type cannot express this
        // statically, but the API contract is enforced by the resolver layer.
        @SuppressWarnings("java:S1948")
        private final LinkedHashMap<String, Object> values;

        private final LinkedHashMap<String, String> referenceFormulationIris;

        private final LinkedHashMap<String, IRI> naturalDatatypes;

        SerializationProxy(EvaluatedValues source) {
            this.values = new LinkedHashMap<>(source.values);
            this.referenceFormulationIris = ReferenceFormulationCodec.toIris(source.referenceFormulations);
            this.naturalDatatypes = new LinkedHashMap<>(source.naturalDatatypes);
        }

        @Serial
        private Object readResolve() throws ObjectStreamException {
            return new EvaluatedValues(
                    values, ReferenceFormulationCodec.fromIris(referenceFormulationIris), naturalDatatypes, null);
        }
    }
}
