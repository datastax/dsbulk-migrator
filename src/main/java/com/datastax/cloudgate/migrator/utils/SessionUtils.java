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

import com.datastax.cloudgate.migrator.settings.ClusterInfo;
import com.datastax.cloudgate.migrator.settings.Credentials;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.GeneralSecurityException;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionUtils.class);

  public static CqlSession createSession(
      ClusterInfo clusterInfo, Credentials credentials, SSLContext sslContext) {
    String clusterName = clusterInfo.isOrigin() ? "origin" : "target";
    try {
      LOGGER.info("Contacting {} cluster...", clusterName);
      CqlSession session = createSessionBuilder(clusterInfo, credentials, sslContext).build();
      LOGGER.info("Successfully contacted {} cluster", clusterName);
      return session;
    } catch (Exception e) {
      throw new IllegalStateException("Could not contact " + clusterName + " cluster", e);
    }
  }

  private static CqlSessionBuilder createSessionBuilder(
      ClusterInfo clusterInfo, Credentials credentials, SSLContext sslContext)
      throws GeneralSecurityException, IOException {
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, "DcInferringLoadBalancingPolicy")
            .build();
    CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(loader);
    if (clusterInfo.isAstra()) {
      builder.withCloudSecureConnectBundle(clusterInfo.getBundle());
    } else {
      List<InetSocketAddress> contactPoints = clusterInfo.getContactPoints();
      builder.addContactPoints(contactPoints);
      // limit connectivity to just the contact points to limit network I/O
      builder.withNodeFilter(
          node -> {
            SocketAddress address = node.getEndPoint().resolve();
            return address instanceof InetSocketAddress && contactPoints.contains(address);
          });
    }
    if (credentials != null) {
      builder.withAuthCredentials(
          credentials.getUsername(), String.valueOf(credentials.getPassword()));
    }
    if (sslContext != null) {
      builder.withSslContext(sslContext);
    }
    return builder;
  }
}
