package io.carml.util;

import io.carml.model.Resource;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LogUtil {

  public static String log(Resource resource) {
    return log(resource, resource);
  }

  public static String log(Collection<? extends Resource> resources) {
    return resources.stream()
        .map(LogUtil::log)
        .collect(Collectors.joining(String.format(",%n")));
  }

  public static String log(Resource ancestor, Resource resource) {
    return ModelSerializer.formatResourceForLog(ancestor.asRdf(), resource.getAsResource(),
        RmlNamespaces.RML_NAMESPACES, false);
  }

  public static String exception(Collection<? extends Resource> resources) {
    return resources.stream()
        .map(LogUtil::exception)
        .collect(Collectors.joining(String.format(",%n")));
  }

  public static String exception(Resource resource) {
    return exception(resource, resource);
  }

  public static String exception(Resource ancestor, Resource resource) {
    return ModelSerializer.formatResourceForLog(ancestor.asRdf(), resource.getAsResource(),
        RmlNamespaces.RML_NAMESPACES, true);
  }
}
