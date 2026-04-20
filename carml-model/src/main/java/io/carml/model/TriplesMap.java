package io.carml.model;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;

public interface TriplesMap extends Resource {

    AbstractLogicalSource getLogicalSource();

    LogicalTable getLogicalTable();

    IRI getBaseIri();

    Set<SubjectMap> getSubjectMaps();

    Set<PredicateObjectMap> getPredicateObjectMaps();

    default Set<String> getReferenceExpressionSet() {
        return getReferenceExpressionSet(getPredicateObjectMaps());
    }

    default Set<String> getReferenceExpressionSet(Set<PredicateObjectMap> predicateObjectMapFilter) {
        var subjectMapExpressions = getSubjectMaps().stream()
                .map(SubjectMap::getReferenceExpressionSet)
                .flatMap(Set::stream);

        var pomExpressions = predicateObjectMapFilter.stream()
                .map(PredicateObjectMap::getReferenceExpressionSet)
                .flatMap(Set::stream);

        return Stream.concat(subjectMapExpressions, pomExpressions).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Returns the union of all {@link LogicalTarget}s declared on any term map in this
     * {@code TriplesMap}. Walks every {@link SubjectMap} (including its {@link GraphMap}s), every
     * {@link PredicateObjectMap} (its {@link PredicateMap}s, {@link BaseObjectMap}s and
     * {@link GraphMap}s). {@link BaseObjectMap}s that are not {@link TermMap}s (e.g.
     * {@code RefObjectMap}) contribute no targets because only {@link TermMap#getLogicalTargets()}
     * is defined.
     *
     * <p>Iteration order is preserved via a {@link LinkedHashSet} so callers get deterministic
     * ordering when they feed the result into maps keyed by {@link LogicalTarget}.
     */
    default Set<LogicalTarget> getAllLogicalTargets() {
        var sink = new LinkedHashSet<LogicalTarget>();
        for (var subjectMap : getSubjectMaps()) {
            sink.addAll(subjectMap.getLogicalTargets());
            subjectMap.getGraphMaps().forEach(gm -> sink.addAll(gm.getLogicalTargets()));
        }
        for (var pom : getPredicateObjectMaps()) {
            pom.getPredicateMaps().forEach(pm -> sink.addAll(pm.getLogicalTargets()));
            pom.getObjectMaps().stream()
                    .filter(TermMap.class::isInstance)
                    .map(TermMap.class::cast)
                    .forEach(om -> sink.addAll(om.getLogicalTargets()));
            pom.getGraphMaps().forEach(gm -> sink.addAll(gm.getLogicalTargets()));
        }
        return Set.copyOf(sink);
    }
}
