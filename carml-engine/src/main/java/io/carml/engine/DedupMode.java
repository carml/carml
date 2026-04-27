package io.carml.engine;

import io.carml.logicalview.DedupStrategy;

/**
 * User-facing override for how deduplication is applied across the mapping pipeline.
 *
 * <p>By default the engine derives a per-view {@link DedupStrategy} from structural annotations
 * (PrimaryKey / Unique+NotNull / NotNull). This enum lets callers override that derivation when
 * they have stronger guarantees about the data, or want stricter correctness at the cost of memory.
 *
 * <ul>
 *   <li>{@link #AUTO} — annotation-driven (current default). PK / covering Unique+NotNull → no
 *       dedup. NotNull on all projected fields → simple-equality dedup. Otherwise → exact dedup.
 *       No annotations → no dedup.
 *   <li>{@link #NONE} — never deduplicate. Fastest; produces duplicate triples when source rows
 *       repeat. Useful when the caller knows the source is row-distinct and wants to skip the
 *       annotation cost, or when downstream pipelines apply their own dedup.
 *   <li>{@link #VIEW} — force at least view-level dedup. PK / covering Unique+NotNull keep the
 *       annotation-derived no-op (dedup is provably unnecessary); the "no annotations" case
 *       escalates from no-op to exact dedup.
 *   <li>{@link #FULL} — view-level dedup as in {@link #VIEW}, plus a statement-level
 *       {@code .distinct()} on the assembled output. Catches cross-TriplesMap duplicate triples
 *       that view-level dedup cannot see. Memory cost grows with the number of distinct output
 *       statements.
 * </ul>
 */
public enum DedupMode {
    AUTO,
    NONE,
    VIEW,
    FULL
}
