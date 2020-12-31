package com.taxonic.carml.rdf_mapper.impl;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

class PropertyUtils {

  private PropertyUtils() {}

  /**
   * Finds the {@code Method} representing the setter specified by {@code setterName}. The algorithm
   * simply gathers all methods in the specified class (inherited or declared) with the specified name
   * that have 1 parameter. Note that the algorithm does not consider the type of the parameter. If
   * multiple such methods exist, a {@code RuntimeException} is thrown.
   *
   * @param c
   * @param setterName
   * @return
   */
  public static Optional<Method> findSetter(Class<?> c, String setterName) {
    List<Method> setters = stream(c.getMethods()).filter(m -> m.getName()
        .equals(setterName))
        .filter(m -> m.getParameterCount() == 1)
        .collect(toList());
    if (setters.isEmpty()) {
      return Optional.empty();
    }
    if (setters.size() > 1) {
      throw new RuntimeException("in class " + c.getCanonicalName() + ", multiple setters with name [" + setterName
          + "] and 1 parameter were found, while expecting only 1");
    }
    return Optional.of(setters.get(0));
  }

  /**
   * @param getterOrSetterName Full name of the getter or setter-method of the property. Example:
   *        {@code getName}.
   * @return
   */
  public static String getPropertyName(String getterOrSetterName) {
    String prefix = getGetterOrSetterPrefix(getterOrSetterName);
    if (prefix == null)
      // no prefix detected - use method name as-is
      return firstToLowerCase(getterOrSetterName);
    return firstToLowerCase(getterOrSetterName.substring(prefix.length()));
  }

  private static String getGetterOrSetterPrefix(String name) {
    return Arrays.asList("set", "get", "is")
        .stream()
        .filter(name::startsWith)
        .filter(p -> startsWithUppercase(name.substring(p.length())))
        .findFirst()
        .orElse(null);
  }

  private static boolean startsWithUppercase(String str) {
    if (str.isEmpty())
      return false;
    String first = str.substring(0, 1);
    return first.equals(first.toUpperCase());
  }

  private static String firstToLowerCase(String str) {
    return transformFirst(str, String::toLowerCase);
  }

  private static String firstToUpperCase(String str) {
    return transformFirst(str, String::toUpperCase);
  }

  private static String transformFirst(String str, UnaryOperator<String> f) {
    if (str.isEmpty())
      return str;
    String first = str.substring(0, 1);
    return f.apply(first) + str.substring(1);
  }

  public static String createSetterName(String property) {
    return "set" + firstToUpperCase(property);
  }

}
