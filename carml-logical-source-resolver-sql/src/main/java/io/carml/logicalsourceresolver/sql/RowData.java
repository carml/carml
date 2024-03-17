package io.carml.logicalsourceresolver.sql;

import io.r2dbc.spi.Type;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor(staticName = "of")
@Getter
public class RowData {

    public static RowData of() {
        return of(Map.of(), Map.of());
    }

    private Map<String, Object> data;

    private Map<String, Type> columnTypes;
}
