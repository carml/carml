package io.carml.logicalview;

import java.util.List;

/**
 * One result of a join probe: a child row paired with the parent {@link ViewIteration}s that
 * matched the join key. Empty {@code matchedParents} signals a no-match for left joins (the child
 * row is still emitted with null-extended join fields downstream); inner joins filter no-match
 * children out before producing a {@code MatchedRow}.
 *
 * @param child the child row to extend with parent values
 * @param matchedParents zero or more parent iterations whose join key matched the child's, in the
 *     order they appeared in the parent stream
 */
public record MatchedRow(EvaluatedValues child, List<ViewIteration> matchedParents) {}
