package io.carml.model;

public interface DatabaseSource extends Source {

    String getJdbcDsn();

    String getJdbcDriver();

    String getUsername();

    CharSequence getPassword();

    String getQuery();
}
