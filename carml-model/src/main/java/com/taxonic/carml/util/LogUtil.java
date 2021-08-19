package com.taxonic.carml.util;

import com.taxonic.carml.model.Resource;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LogUtil {

  public static String log(Resource resource) {
    return log(resource, resource);
  }

  public static String log(Resource ancestor, Resource resource) {
    return ModelSerializer.formatResourceForLog(ancestor.asRdf(), resource.getAsResource(),
        RmlNamespaces.RML_NAMESPACES, false);
  }

  public static String exception(Resource resource) {
    return exception(resource, resource);
  }

  public static String exception(Resource ancestor, Resource resource) {
    return ModelSerializer.formatResourceForLog(ancestor.asRdf(), resource.getAsResource(),
        RmlNamespaces.RML_NAMESPACES, true);
  }
}
