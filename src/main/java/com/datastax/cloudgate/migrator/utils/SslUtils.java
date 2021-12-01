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

import com.datastax.cloudgate.migrator.settings.SslStore;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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

  public static SSLContext createSslContext(SslStore keystore, SslStore truststore) {
    try {
      KeyManagerFactory kmf = null;
      if (keystore != null) {
        kmf = createKeyManagerFactory(keystore.getPath(), keystore.getPassword());
      }
      TrustManagerFactory tmf = null;
      if (truststore != null) {
        tmf = createTrustManagerFactory(truststore.getPath(), truststore.getPassword());
      }
      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(
          kmf != null ? kmf.getKeyManagers() : null,
          tmf != null ? tmf.getTrustManagers() : null,
          new SecureRandom());
      return sslContext;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static KeyManagerFactory createKeyManagerFactory(
      Path keystorePath, char[] keystorePassword) throws IOException, GeneralSecurityException {
    KeyStore ks = KeyStore.getInstance("JKS");
    try (InputStream kis = Files.newInputStream(keystorePath)) {
      ks.load(kis, keystorePassword);
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, keystorePassword);
      return kmf;
    }
  }

  private static TrustManagerFactory createTrustManagerFactory(
      Path truststorePath, char[] truststorePassword) throws IOException, GeneralSecurityException {
    KeyStore ts = KeyStore.getInstance("JKS");
    try (InputStream tis = Files.newInputStream(truststorePath)) {
      ts.load(tis, truststorePassword);
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ts);
      return tmf;
    }
  }
}
