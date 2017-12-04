package com.taxonic.carml.engine;

import java.util.Optional;
import java.util.function.Function;

public interface SourceResolver extends Function<Object, Optional<String>> {
	
	void setSourceManager(LogicalSourceManager sourceManager);
}
