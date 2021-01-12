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
import com.datastax.cloudgate.migrator.settings.ExportSettings;
import com.datastax.cloudgate.migrator.settings.ImportSettings;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionUtils.class);

  public static CqlSession createExportSession(ExportSettings exportSettings) {
    try {
      LOGGER.info("Contacting origin cluster...");
      CqlSession session = createSession(exportSettings.clusterInfo, exportSettings.credentials);
      LOGGER.info("Origin cluster successfully contacted");
      return session;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Could not reach origin cluster; please check your --export-host or --export-bundle values",
          e);
    }
  }

  public static CqlSession createImportSession(ImportSettings importSettings) {
    try {
      LOGGER.info("Contacting target cluster...");
      CqlSession session = createSession(importSettings.clusterInfo, importSettings.credentials);
      LOGGER.info("Target cluster successfully contacted");
      return session;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Could not reach target cluster; please check your --import-host or --import-bundle values",
          e);
    }
  }

  private static CqlSession createSession(ClusterInfo clusterInfo, Credentials credentials) {
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, "DcInferringLoadBalancingPolicy")
            .build();
    CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(loader);
    if (clusterInfo.isAstra()) {
      builder.withCloudSecureConnectBundle(clusterInfo.getBundle());
    } else {
      InetSocketAddress contactPoint = clusterInfo.getHostAddress();
      builder.addContactPoint(contactPoint);
      // limit connectivity to just the contact host to limit network I/O
      builder.withNodeFilter(node -> node.getEndPoint().resolve().equals(contactPoint));
    }
    if (credentials != null) {
      builder.withAuthCredentials(
          credentials.getUsername(), String.valueOf(credentials.getPassword()));
    }
    return builder.build();
  }
}
