package com.taxonic.carml.rdf_mapper;

import java.util.List;

public interface Combiner<T> {

    T combine(List<T> delegateInvocationResults);

}
