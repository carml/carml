package io.carml.logicalview;

import io.carml.model.ReferenceFormulation;
import io.carml.model.impl.CarmlReferenceFormulation;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serialization helper for {@link ReferenceFormulation} maps. {@link ReferenceFormulation}
 * instances are not {@link java.io.Serializable} (their {@code CarmlResource} superclass is not),
 * so wire formats convert them to IRI strings and reconstruct minimal
 * {@link CarmlReferenceFormulation} instances on read. Downstream consumers (notably the join
 * executor stack) only need IRI identity — full RDF annotations are not required.
 *
 * <p>Used by both the in-module {@code SerializationProxy}s in {@link EvaluatedValues} and
 * {@link DefaultViewIteration} and by sibling-module wire formats (e.g. the Arrow IPC codec in
 * the DuckDB join executor module).
 */
public final class ReferenceFormulationCodec {

    private ReferenceFormulationCodec() {}

    /**
     * Encodes a map of reference formulations as IRI strings, preserving insertion order. Returns
     * an empty map if the input is {@code null}.
     *
     * @param source the reference-formulation map (may be {@code null})
     * @return a new {@link LinkedHashMap} of IRI strings keyed by the source keys
     */
    public static Map<String, String> toIris(Map<String, ReferenceFormulation> source) {
        if (source == null) {
            return new LinkedHashMap<>();
        }
        return source.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getAsResource().stringValue(),
                        (a, b) -> a,
                        LinkedHashMap::new));
    }

    /**
     * Reconstructs a map of {@link ReferenceFormulation} instances from IRI strings, preserving
     * insertion order. Returns an empty map if the input is {@code null}.
     *
     * @param iris the IRI-string map (may be {@code null})
     * @return a new {@link LinkedHashMap} of {@link ReferenceFormulation} instances keyed by the
     *     source keys
     */
    public static Map<String, ReferenceFormulation> fromIris(Map<String, String> iris) {
        if (iris == null) {
            return new LinkedHashMap<>();
        }
        return iris.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> CarmlReferenceFormulation.builder()
                                .id(e.getValue())
                                .build(),
                        (a, b) -> a,
                        LinkedHashMap::new));
    }
}
