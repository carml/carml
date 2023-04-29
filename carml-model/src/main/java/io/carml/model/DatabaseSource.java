package io.carml.model;

public interface DatabaseSource extends Resource {

  String getJdbcDsn();

  String getJdbcDriver();

  String getUsername();

  CharSequence getPassword();

  String getQuery();
}
