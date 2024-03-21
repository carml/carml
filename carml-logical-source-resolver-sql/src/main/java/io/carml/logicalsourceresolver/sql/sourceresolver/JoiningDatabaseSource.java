package io.carml.logicalsourceresolver.sql.sourceresolver;

import io.carml.model.LogicalSource;
import io.carml.model.RefObjectMap;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class JoiningDatabaseSource {

    @NonNull
    private LogicalSource childLogicalSource;

    @NonNull
    private LogicalSource parentLogicalSource;

    @NonNull
    private Set<RefObjectMap> refObjectMaps;

    @NonNull
    private Set<String> childExpressions;

    @NonNull
    private Set<String> parentExpressions;
}
