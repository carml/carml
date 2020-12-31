package com.taxonic.carml.engine;

import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TermMap;
import com.taxonic.carml.model.TermType;
import java.util.Optional;
import java.util.Set;

public interface TermGeneratorFactory<T> {

  TermGenerator<? extends T> getSubjectGenerator(SubjectMap subjectMap);

  TermGenerator<? extends T> getPredicateGenerator(PredicateMap predicateMap);

  TermGenerator<? extends T> getObjectGenerator(ObjectMap objectMap);

  TermGenerator<? extends T> getGraphGenerator(GraphMap graphMap);

  Optional<TermGenerator<? extends T>> getConstantGenerator(TermMap map, Set<Class<? extends T>> allowedConstantTypes);

  Optional<TermGenerator<? extends T>> getReferenceGenerator(TermMap map, Set<TermType> allowedTermTypes);

  Optional<TermGenerator<? extends T>> getTemplateGenerator(TermMap map, Set<TermType> allowedTermTypes);

  Optional<TermGenerator<? extends T>> getFunctionValueGenerator(TermMap map, Set<TermType> allowedTermTypes);

}
