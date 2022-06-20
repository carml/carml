package io.carml.model;


public interface ObjectMap extends TermMap, BaseObjectMap {

  DatatypeMap getDatatypeMap();

  LanguageMap getLanguageMap();

}
