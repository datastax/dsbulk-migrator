package com.datastax.cloudgate.migrator.settings;

public interface TlsSettings {

    boolean useTls();

    String getTruststorePath();

    char[] getTruststorePassword();

    boolean performHostnameValidation();
}
