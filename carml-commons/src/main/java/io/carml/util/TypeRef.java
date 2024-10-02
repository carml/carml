package io.carml.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public abstract class TypeRef<T> {

    private final Type type;

    protected TypeRef() {
        Type superClass = getClass().getGenericSuperclass();
        this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    private TypeRef(Type type) {
        this.type = type;
    }

    public static <T> TypeRef<T> forType(Type type) {
        return new TypeRef<>(type) {};
    }

    public static <T> TypeRef<T> forClass(Class<T> clazz) {
        return new TypeRef<>(clazz) {};
    }

    @Override
    public boolean equals(Object other) {
        return (this == other || (other instanceof TypeRef<?> that && this.type.equals(that.type)));
    }

    @Override
    public int hashCode() {
        return this.type.hashCode();
    }
}
