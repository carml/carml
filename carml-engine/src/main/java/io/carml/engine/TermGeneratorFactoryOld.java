package io.carml.engine;

import io.carml.model.DatatypeMap;
import io.carml.model.ExpressionMap;
import io.carml.model.GraphMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.SubjectMap;
import io.carml.model.TermType;
import java.util.Optional;
import java.util.Set;

@SuppressWarnings("java:S1452")
public interface TermGeneratorFactoryOld<T> {

    TermGenerator<? extends T> getSubjectGenerator(SubjectMap subjectMap);

    TermGenerator<? extends T> getPredicateGenerator(PredicateMap predicateMap);

    TermGenerator<? extends T> getObjectGenerator(ObjectMap objectMap);

    TermGenerator<? extends T> getGraphGenerator(GraphMap graphMap);

    TermGenerator<? extends T> getDatatypeGenerator(DatatypeMap datatypeMap);

    Optional<TermGenerator<? extends T>> getConstantGenerator(
            ExpressionMap map, Set<Class<? extends T>> allowedConstantTypes);

    Optional<TermGenerator<? extends T>> getReferenceGenerator(ExpressionMap map, Set<TermType> allowedTermTypes);

    Optional<TermGenerator<? extends T>> getTemplateGenerator(ExpressionMap map, Set<TermType> allowedTermTypes);

    Optional<TermGenerator<? extends T>> getFunctionValueGenerator(ExpressionMap map, Set<TermType> allowedTermTypes);
}
