package io.carml.engine.function;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Converts values to target types for function parameter binding. */
public interface TypeCoercer {

    /** Returns whether this coercer can convert values to the given target type. */
    boolean supports(Class<?> targetType);

    /** Coerces the given value to the target type. */
    Object coerce(Object value, Class<?> targetType);

    /** Returns a coercer that converts values to {@link String} via {@link Object#toString()}. */
    static TypeCoercer string() {
        return new TypeCoercer() {
            @Override
            public boolean supports(Class<?> targetType) {
                return String.class.equals(targetType);
            }

            @Override
            public Object coerce(Object value, Class<?> targetType) {
                if (value == null) {
                    return null;
                }
                return value.toString();
            }
        };
    }

    /** Returns a coercer that handles numeric conversions (int, long, double, float). */
    static TypeCoercer numeric() {
        Map<Class<?>, Function<Number, Object>> fromNumber = Map.of(
                Integer.class, Number::intValue,
                Integer.TYPE, Number::intValue,
                Long.class, Number::longValue,
                Long.TYPE, Number::longValue,
                Double.class, Number::doubleValue,
                Double.TYPE, Number::doubleValue,
                Float.class, Number::floatValue,
                Float.TYPE, Number::floatValue);

        Map<Class<?>, Function<String, Object>> fromString = Map.of(
                Integer.class, Integer::parseInt,
                Integer.TYPE, Integer::parseInt,
                Long.class, Long::parseLong,
                Long.TYPE, Long::parseLong,
                Double.class, Double::parseDouble,
                Double.TYPE, Double::parseDouble,
                Float.class, Float::parseFloat,
                Float.TYPE, Float::parseFloat);

        return new TypeCoercer() {
            @Override
            public boolean supports(Class<?> targetType) {
                return fromNumber.containsKey(targetType);
            }

            @Override
            public Object coerce(Object value, Class<?> targetType) {
                if (value == null) {
                    return null;
                }
                if (value instanceof Number number) {
                    return fromNumber.get(targetType).apply(number);
                }
                return fromString.get(targetType).apply(value.toString());
            }
        };
    }

    /** Returns a coercer that handles boolean conversion. */
    static TypeCoercer bool() {
        return new TypeCoercer() {
            @Override
            public boolean supports(Class<?> targetType) {
                return Boolean.class.equals(targetType) || Boolean.TYPE.equals(targetType);
            }

            @Override
            public Object coerce(Object value, Class<?> targetType) {
                if (value == null) {
                    return null;
                }
                if (value instanceof Boolean boolValue) {
                    return boolValue;
                }
                return Boolean.parseBoolean(value.toString());
            }
        };
    }

    /** Returns a coercer that handles {@link Collection} types. Wraps single values in a list. */
    static TypeCoercer collection() {
        return new TypeCoercer() {
            @Override
            public boolean supports(Class<?> targetType) {
                return Collection.class.isAssignableFrom(targetType);
            }

            @Override
            public Object coerce(Object value, Class<?> targetType) {
                if (value == null) {
                    return null;
                }
                if (value instanceof Collection<?> c) {
                    return c;
                }
                return List.of(value);
            }
        };
    }

    /** Returns a composite coercer. The first coercer that supports the target type wins. */
    static TypeCoercer composite(List<TypeCoercer> coercers) {
        var immutableCoercers = List.copyOf(coercers);

        return new TypeCoercer() {
            @Override
            public boolean supports(Class<?> targetType) {
                return immutableCoercers.stream().anyMatch(coercer -> coercer.supports(targetType));
            }

            @Override
            public Object coerce(Object value, Class<?> targetType) {
                var matched = immutableCoercers.stream()
                        .filter(coercer -> coercer.supports(targetType))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException(
                                "No coercer found for target type: %s".formatted(targetType)));
                return matched.coerce(value, targetType);
            }
        };
    }

    /** Returns a catch-all coercer that converts values to their string representation. */
    static TypeCoercer passthrough() {
        return new TypeCoercer() {
            @Override
            public boolean supports(Class<?> targetType) {
                return true;
            }

            @Override
            public Object coerce(Object value, Class<?> targetType) {
                if (value == null) {
                    return null;
                }
                return value.toString();
            }
        };
    }

    /** Returns a composite coercer combining string, numeric, boolean, collection, and passthrough coercers. */
    static TypeCoercer defaults() {
        return composite(List.of(string(), numeric(), bool(), collection(), passthrough()));
    }
}
