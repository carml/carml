package io.carml.logicalview;

import io.carml.model.ReferenceFormulation;
import io.carml.model.impl.CarmlReferenceFormulation;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serialization helper for {@link ReferenceFormulation} maps. {@link ReferenceFormulation}
 * instances are not {@link java.io.Serializable} (their {@code CarmlResource} superclass is not),
 * so the {@code SerializationProxy} pattern in {@link EvaluatedValues} and
 * {@link DefaultViewIteration} converts them to IRI strings on the wire and reconstructs minimal
 * {@link CarmlReferenceFormulation} instances on read. Downstream consumers of deserialized
 * iterations (notably the join executor stack) only need IRI identity — full RDF annotations are
 * not required.
 */
final class ReferenceFormulationCodec {

    private ReferenceFormulationCodec() {}

    /**
     * Encodes a map of reference formulations as IRI strings, preserving insertion order.
     * Returns an empty map if the input is {@code null}.
     */
    static LinkedHashMap<String, String> toIris(Map<String, ReferenceFormulation> source) {
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
     */
    static LinkedHashMap<String, ReferenceFormulation> fromIris(Map<String, String> iris) {
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
