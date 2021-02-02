package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.CreateContextEvaluate;
import com.taxonic.carml.model.ContextEntry;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class NestedMapper<T> {

    private static final Logger LOG = LoggerFactory.getLogger(NestedMapper.class);

    private final ContextTriplesMapper<T> triplesMapper;
    private final Set<ContextEntry> contextEntries;
    private CreateContextEvaluate createContextEvaluate;

    public NestedMapper(
        ContextTriplesMapper<T> triplesMapper,
        Set<ContextEntry> contextEntries,
        CreateContextEvaluate createContextEvaluate
    ) {
        this.triplesMapper = triplesMapper;
        this.contextEntries = contextEntries;
        this.createContextEvaluate = createContextEvaluate;
    }

    Set<Resource> map(Model model, EvaluateExpression evaluate) {
        return triplesMapper.map(model, createContextEvaluate(evaluate));
    }

    private EvaluateExpression createContextEvaluate(EvaluateExpression evaluate) {
        return contextEntries.isEmpty()
            ? evaluate
            : createContextEvaluate.apply(contextEntries, evaluate);
    }
}
