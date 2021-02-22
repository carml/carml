package com.taxonic.carml.model;

import java.util.Set;

public interface MergeSuper extends Resource {

    LogicalSource getLogicalSource();

    Set<ContextEntry> getIncluding();

}
