package com.taxonic.carml.engine.source_resolver;

import com.taxonic.carml.engine.LogicalSourceManager;
import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.model.NameableStream;

import java.util.Optional;

public class CarmlStreamResolver implements SourceResolver {

    private LogicalSourceManager sourceManager;

    @Override
    public void setSourceManager(LogicalSourceManager sourceManager) {
        this.sourceManager = sourceManager;
    }

    @Override
    public Optional<String> apply(Object o) {

        if (!(o instanceof NameableStream)) {
            return Optional.empty();
        }

        NameableStream stream = (NameableStream) o;
        Optional<String> name = Optional.ofNullable(stream.getStreamName());
        String resolved =
            name.isPresent() ?
                sourceManager.getSource(name.get()) :
                sourceManager.getSource(RmlMapper.DEFAULT_STREAM_NAME);
        return Optional.of(resolved);
    }
}
