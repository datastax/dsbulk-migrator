/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cloudgate.migrator.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class SslUtils {

  public static SSLContext createSslContext(
      Path keystorePath, char[] keystorePassword, Path truststorePath, char[] truststorePassword)
      throws GeneralSecurityException, IOException {
    KeyManagerFactory kmf = null;
    if (keystorePath != null && keystorePassword != null) {
      kmf = createKeyManagerFactory(keystorePath, keystorePassword);
    }
    TrustManagerFactory tmf = createTrustManagerFactory(truststorePath, truststorePassword);
    SSLContext sslContext = SSLContext.getInstance("SSL");
    sslContext.init(
        kmf != null ? kmf.getKeyManagers() : null, tmf.getTrustManagers(), new SecureRandom());
    return sslContext;
  }

  private static KeyManagerFactory createKeyManagerFactory(
      Path keystorePath, char[] keystorePassword) throws IOException, GeneralSecurityException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(Files.newInputStream(keystorePath), keystorePassword);
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, keystorePassword);
    Arrays.fill(keystorePassword, (char) 0);
    return kmf;
  }

  private static TrustManagerFactory createTrustManagerFactory(
      Path truststorePath, char[] truststorePassword) throws IOException, GeneralSecurityException {
    KeyStore ts = KeyStore.getInstance("JKS");
    ts.load(Files.newInputStream(truststorePath), truststorePassword);
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);
    Arrays.fill(truststorePassword, (char) 0);
    return tmf;
  }
}
