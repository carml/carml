package io.carml.engine;

import io.carml.model.GraphMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.SubjectMap;

@SuppressWarnings("java:S1452")
public interface TermGeneratorFactory<T> {

  TermGenerator<? extends T> getSubjectGenerator(SubjectMap subjectMap);

  TermGenerator<? extends T> getPredicateGenerator(PredicateMap predicateMap);

  TermGenerator<? extends T> getObjectGenerator(ObjectMap objectMap);

  TermGenerator<? extends T> getGraphGenerator(GraphMap graphMap);
}
