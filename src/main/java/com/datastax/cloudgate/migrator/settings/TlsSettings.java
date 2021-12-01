package com.datastax.cloudgate.migrator.settings;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.GeneralSecurityException;

public interface TlsSettings {

    SSLContext getSslContext() throws GeneralSecurityException, IOException;

    boolean performHostnameValidation();

    String[] getCipherSuites();
}
