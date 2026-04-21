package io.carml.model;

import org.eclipse.rdf4j.model.Value;

/**
 * File-based addressing of a resource. Per the RML-IO specification, {@code rml:FilePath} is dual-use
 * — it addresses both input {@link Source}s (logical source data files) and output {@link Target}s
 * (logical target sinks). Accordingly, {@code FilePath} extends both interfaces so that a single
 * {@code rml:FilePath} resource can be assigned to either role without type gymnastics in the
 * rdf-mapper (the generated proxy transparently implements both supertypes).
 *
 * <p>The {@link #getEncoding()} and {@link #getCompression()} signatures unify cleanly because
 * {@link Source} and {@link Target} both declare them with identical return types.
 * {@link Target#getSerialization()} lives on {@code rml:LogicalTarget} in the data model and is
 * returned as {@code null} here — see {@code CarmlFilePath.getSerialization()} for details.
 *
 * <p><strong>Combined-typed nested targets:</strong> when a mapping declares
 * {@code rml:target [ a rml:Target, rml:FilePath ]} and {@code rml:serialization}/
 * {@code rml:encoding}/{@code rml:compression} are declared on the nested target instance itself
 * (rather than on the enclosing {@code rml:LogicalTarget}), multi-delegate dispatch order inside
 * {@code CarmlMapper.doMultipleInterfaceMapping} determines which delegate's getter responds.
 * The implementation set iterates as {@link java.util.HashSet}, and empirically the iteration
 * order varies across runs: running {@code RmlMappingLoaderTest} in isolation surfaces the
 * {@link Target} delegate (which returns the declared IRI), while running the full
 * {@code carml-model} test suite surfaces the {@link FilePath} delegate (whose
 * {@code CarmlFilePath.getSerialization()} returns {@code null} by contract). This is
 * observable non-determinism, not a formally specified behavior. RML-IO-conforming mappings
 * declare these at the {@code LogicalTarget} level; tooling should prefer
 * {@code LogicalTarget.getSerialization()} with a fallback to {@link Target#getSerialization()}
 * only when the nested form is used deliberately, and in that case treat the result as
 * best-effort.
 */
public interface FilePath extends Source, Target {

    Value getRoot();

    String getPath();
}
