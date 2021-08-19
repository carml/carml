package com.taxonic.carml.engine;

import com.taxonic.carml.model.ExpressionMap;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TermType;
import java.util.Optional;
import java.util.Set;

@SuppressWarnings("java:S1452")
public interface TermGeneratorFactory<T> {

  TermGenerator<? extends T> getSubjectGenerator(SubjectMap subjectMap);

  TermGenerator<? extends T> getPredicateGenerator(PredicateMap predicateMap);

  TermGenerator<? extends T> getObjectGenerator(ObjectMap objectMap);

  TermGenerator<? extends T> getGraphGenerator(GraphMap graphMap);

  Optional<TermGenerator<? extends T>> getConstantGenerator(ExpressionMap map,
      Set<Class<? extends T>> allowedConstantTypes);

  Optional<TermGenerator<? extends T>> getReferenceGenerator(ExpressionMap map, Set<TermType> allowedTermTypes);

  Optional<TermGenerator<? extends T>> getTemplateGenerator(ExpressionMap map, Set<TermType> allowedTermTypes);

  Optional<TermGenerator<? extends T>> getFunctionValueGenerator(ExpressionMap map, Set<TermType> allowedTermTypes);

}
