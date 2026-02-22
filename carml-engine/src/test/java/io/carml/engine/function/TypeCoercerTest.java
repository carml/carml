package io.carml.engine.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TypeCoercerTest {

    @Nested
    class StringCoercer {

        private final TypeCoercer coercer = TypeCoercer.string();

        @Test
        void supports_returnsTrue_forString() {
            assertThat(coercer.supports(String.class), is(true));
        }

        @Test
        void supports_returnsFalse_forInteger() {
            assertThat(coercer.supports(Integer.class), is(false));
        }

        @Test
        void coerce_returnsString_givenInt() {
            assertThat(coercer.coerce(42, String.class), is("42"));
        }

        @Test
        void coerce_returnsNull_givenNull() {
            assertThat(coercer.coerce(null, String.class), is(nullValue()));
        }
    }

    @Nested
    class NumericCoercer {

        private final TypeCoercer coercer = TypeCoercer.numeric();

        @Test
        void supports_returnsTrue_forIntegerClass() {
            assertThat(coercer.supports(Integer.class), is(true));
        }

        @Test
        void supports_returnsTrue_forIntType() {
            assertThat(coercer.supports(Integer.TYPE), is(true));
        }

        @Test
        void supports_returnsTrue_forLongClass() {
            assertThat(coercer.supports(Long.class), is(true));
        }

        @Test
        void supports_returnsTrue_forLongType() {
            assertThat(coercer.supports(Long.TYPE), is(true));
        }

        @Test
        void supports_returnsTrue_forDoubleClass() {
            assertThat(coercer.supports(Double.class), is(true));
        }

        @Test
        void supports_returnsTrue_forDoubleType() {
            assertThat(coercer.supports(Double.TYPE), is(true));
        }

        @Test
        void supports_returnsTrue_forFloatClass() {
            assertThat(coercer.supports(Float.class), is(true));
        }

        @Test
        void supports_returnsTrue_forFloatType() {
            assertThat(coercer.supports(Float.TYPE), is(true));
        }

        @Test
        void supports_returnsFalse_forStringClass() {
            assertThat(coercer.supports(String.class), is(false));
        }

        @Test
        void coerce_returnsIntValue_givenNumberAndIntegerTarget() {
            assertThat(coercer.coerce(42L, Integer.class), is(42));
        }

        @Test
        void coerce_returnsLongValue_givenNumberAndLongTarget() {
            assertThat(coercer.coerce(42, Long.class), is(42L));
        }

        @Test
        void coerce_returnsDoubleValue_givenStringAndDoubleTarget() {
            assertThat(coercer.coerce("3.14", Double.class), is(3.14));
        }

        @Test
        void coerce_returnsFloatValue_givenStringAndFloatTarget() {
            assertThat(coercer.coerce("1.5", Float.class), is(1.5f));
        }

        @Test
        void coerce_returnsNull_givenNull() {
            assertThat(coercer.coerce(null, Integer.class), is(nullValue()));
        }
    }

    @Nested
    class BoolCoercer {

        private final TypeCoercer coercer = TypeCoercer.bool();

        @Test
        void supports_returnsTrue_forBooleanClass() {
            assertThat(coercer.supports(Boolean.class), is(true));
        }

        @Test
        void supports_returnsTrue_forBooleanType() {
            assertThat(coercer.supports(Boolean.TYPE), is(true));
        }

        @Test
        void coerce_returnsTrue_givenTrueString() {
            assertThat(coercer.coerce("true", Boolean.class), is(true));
        }

        @Test
        void coerce_returnsFalse_givenFalseString() {
            assertThat(coercer.coerce("false", Boolean.class), is(false));
        }

        @Test
        void coerce_returnsBooleanValue_givenBoolean() {
            assertThat(coercer.coerce(Boolean.TRUE, Boolean.class), is(true));
        }

        @Test
        void coerce_returnsNull_givenNull() {
            assertThat(coercer.coerce(null, Boolean.class), is(nullValue()));
        }
    }

    @Nested
    class CollectionCoercer {

        private final TypeCoercer coercer = TypeCoercer.collection();

        @Test
        void supports_returnsTrue_forCollection() {
            assertThat(coercer.supports(Collection.class), is(true));
        }

        @Test
        void supports_returnsTrue_forList() {
            assertThat(coercer.supports(List.class), is(true));
        }

        @Test
        void supports_returnsTrue_forSet() {
            assertThat(coercer.supports(Set.class), is(true));
        }

        @Test
        void coerce_passesThroughCollection() {
            var input = List.of("a", "b");

            assertThat(coercer.coerce(input, Collection.class), is(input));
        }

        @Test
        void coerce_wrapsSingleValue_inList() {
            var result = coercer.coerce("single", Collection.class);

            assertThat(result, instanceOf(List.class));
            assertThat((List<?>) result, contains("single"));
        }

        @Test
        void coerce_returnsNull_givenNull() {
            assertThat(coercer.coerce(null, Collection.class), is(nullValue()));
        }
    }

    @Nested
    class CompositeCoercer {

        @Test
        void coerce_usesFirstMatchingCoercer() {
            var coercer = TypeCoercer.composite(List.of(TypeCoercer.string(), TypeCoercer.numeric()));

            assertThat(coercer.coerce(42, String.class), is("42"));
        }

        @Test
        void coerce_throwsIllegalArgument_forUnsupportedType() {
            var coercer = TypeCoercer.composite(List.of(TypeCoercer.string()));

            assertThrows(IllegalArgumentException.class, () -> coercer.coerce(42, Integer.class));
        }
    }

    @Nested
    class DefaultsCoercer {

        private final TypeCoercer coercer = TypeCoercer.defaults();

        @Test
        void supports_string() {
            assertThat(coercer.supports(String.class), is(true));
        }

        @Test
        void supports_numericTypes() {
            assertThat(coercer.supports(Integer.class), is(true));
            assertThat(coercer.supports(Long.class), is(true));
            assertThat(coercer.supports(Double.class), is(true));
        }

        @Test
        void supports_boolean() {
            assertThat(coercer.supports(Boolean.class), is(true));
        }

        @Test
        void supports_collection() {
            assertThat(coercer.supports(Collection.class), is(true));
        }

        @Test
        void supports_objectClass() {
            assertThat(coercer.supports(Object.class), is(true));
        }

        @Test
        void coerce_returnsStringValue_givenObjectClassTarget() {
            assertThat(coercer.coerce("hello", Object.class), is("hello"));
        }

        @Test
        void coerce_returnsToString_givenObjectClassTargetAndNonString() {
            assertThat(coercer.coerce(42, Object.class), is("42"));
        }

        @Test
        void coerce_returnsNull_givenObjectClassTargetAndNull() {
            assertThat(coercer.coerce(null, Object.class), is(nullValue()));
        }
    }

    @Nested
    class PassthroughCoercer {

        private final TypeCoercer coercer = TypeCoercer.passthrough();

        @Test
        void supports_returnsTrue_forAnyType() {
            assertThat(coercer.supports(Object.class), is(true));
            assertThat(coercer.supports(String.class), is(true));
            assertThat(coercer.supports(Integer.class), is(true));
        }

        @Test
        void coerce_returnsToString_givenAnyValue() {
            assertThat(coercer.coerce("hello", Object.class), is("hello"));
            assertThat(coercer.coerce(42, Object.class), is("42"));
        }

        @Test
        void coerce_returnsNull_givenNull() {
            assertThat(coercer.coerce(null, Object.class), is(nullValue()));
        }
    }
}
