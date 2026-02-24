package io.carml.util;

import java.util.ArrayList;
import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CartesianProduct {

    public static <T> List<List<T>> listCartesianProduct(List<List<T>> lists) {
        List<List<T>> result = new ArrayList<>();
        result.add(List.of());
        for (List<T> list : lists) {
            List<List<T>> newResult = new ArrayList<>();
            for (List<T> existing : result) {
                for (T element : list) {
                    var combined = new ArrayList<T>(existing.size() + 1);
                    combined.addAll(existing);
                    combined.add(element);
                    newResult.add(List.copyOf(combined));
                }
            }
            result = newResult;
        }
        return List.copyOf(result);
    }
}
