package io.carml.logicalsourceresolver.sql.sourceresolver;

import io.carml.model.LogicalSource;
import io.carml.model.RefObjectMap;
import io.carml.model.impl.CarmlDatabaseSource;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode(callSuper = false)
public class JoiningDatabaseSource extends CarmlDatabaseSource {

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
