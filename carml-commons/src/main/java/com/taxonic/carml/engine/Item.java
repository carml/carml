package com.taxonic.carml.engine;

import java.util.Objects;

public class Item<T> {

    @SuppressWarnings("squid:S1700")
    private final T item;
    private final EvaluateExpression evaluate;

    public Item(T item, EvaluateExpression evaluate) {
        this.item = item;
        this.evaluate = evaluate;
    }

    // TODO this only exists for logging, really. we may choose
    //      to log the item on a different level and remove class Item
    public T getItem() {
        return item;
    }

    public EvaluateExpression getEvaluate() {
        return evaluate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Item<?> item1 = (Item<?>) o;
        return Objects.equals(item, item1.item) && Objects.equals(evaluate, item1.evaluate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(item, evaluate);
    }

    @Override
    public String toString() {
        return "Item{" +
            "item=" + item +
            ", evaluate=" + evaluate +
            '}';
    }
}
