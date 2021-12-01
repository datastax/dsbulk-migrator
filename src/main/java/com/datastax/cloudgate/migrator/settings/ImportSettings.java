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
package com.datastax.cloudgate.migrator.settings;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ImportSettings {

  @ArgGroup(multiplicity = "1")
  public ImportClusterInfo clusterInfo;

  public static class ImportClusterInfo implements ClusterInfo {

    @Option(
        names = "--import-host",
        paramLabel = "HOST[:PORT]",
        description =
            "The host name or IP and, optionally, the port of a node from the target cluster. "
                + "If the port is not specified, it will default to 9042. "
                + "This option can be specified multiple times. "
                + "Options --export-host and --export-bundle are mutually exclusive.",
        converter = HostAndPortConverter.class,
        required = true)
    public List<HostAndPort> hostsAndPorts;

    @Option(
        names = "--import-bundle",
        paramLabel = "PATH",
        description =
            "The path to a secure connect bundle to connect to the target cluster, "
                + "if that cluster is a DataStax Astra cluster. "
                + "Options --export-host and --export-bundle are mutually exclusive.",
        required = true)
    public Path bundle;

    @Override
    public boolean isOrigin() {
      return false;
    }

    @Override
    public List<InetSocketAddress> getContactPoints() {
      if (hostsAndPorts == null || hostsAndPorts.isEmpty()) {
        return Collections.emptyList();
      }
      return hostsAndPorts.stream()
          .map(
              hp ->
                  InetSocketAddress.createUnresolved(
                      hp.getHost(), hp.hasPort() ? hp.getPort() : 9042))
          .collect(Collectors.toList());
    }

    @Override
    public Path getBundle() {
      return bundle;
    }
  }

  @ArgGroup(exclusive = false)
  public ImportCredentials credentials;

  public static class ImportCredentials implements Credentials {

    @Option(
        names = "--import-username",
        paramLabel = "STRING",
        description =
            "The username to use to authenticate against the target cluster. "
                + "Options --export-username and --export-password must be provided together, or not at all.",
        required = true)
    public String username;

    @Option(
        names = "--import-password",
        paramLabel = "STRING",
        description =
            "The password to use to authenticate against the target cluster. "
                + "Options --export-username and --export-password must be provided together, or not at all. "
                + "Omit the parameter value to be prompted for the password interactively.",
        required = true,
        prompt = "Enter the password to use to authenticate against the target cluster: ",
        interactive = true)
    public char[] password;

    @Override
    public String getUsername() {
      return username;
    }

    @Override
    public char[] getPassword() {
      return password;
    }
  }

  @ArgGroup(exclusive = false)
  public ImportTlsSettings tlsSettings;

  public static class ImportTlsSettings implements TlsSettings {

    @Option(
        names = "--import-use-tls",
        paramLabel = "BOOLEAN",
        description =
            "Whether TLS be used when connecting to the target cluster. If left to default or set to false, all other TLS-related parameters are ignored.",
        required = false,
        defaultValue = "false")
    public boolean useTls = false;

    @Option(
        names = "--import-tls-truststore-path",
        paramLabel = "STRING",
        description =
            "Path of the truststore to connect to the target cluster. Only relevant when connecting to a cluster requiring TLS.")
    public String truststorePath = "";

    @Option(
        names = "--import-tls-truststore-password",
        paramLabel = "STRING",
        description =
            "The password of the truststore used to connect to the target cluster.  Only relevant when connecting to a cluster requiring TLS."
                + "Should only be provided if specifying a password-protected truststore. "
                + "Omit the parameter value to be prompted for the password interactively. "
                + "If the truststore does not require a password, when prompted for it just press enter",
        required = false,
        prompt = "Enter the password for the truststore to connect the target cluster: ",
        interactive = true)
    public char[] truststorePassword;

    @Option(
        names = "--import-tls-hostname-validation",
        paramLabel = "BOOLEAN",
        description =
            "Whether hostname validation should be performed when connecting to the target cluster. Only relevant when connecting to a cluster requiring TLS.",
        defaultValue = "true")
    public boolean performHostnameValidation = true;

    @Override
    public boolean useTls() {
      return useTls;
    }

    @Override
    public String getTruststorePath() {
      return truststorePath;
    }

    @Override
    public char[] getTruststorePassword() {
      return truststorePassword;
    }

    @Override
    public boolean performHostnameValidation() {
      return performHostnameValidation;
    }
  }

  @Option(
      names = "--import-consistency",
      paramLabel = "CONSISTENCY",
      description =
          "The consistency level to use when importing data. The default is LOCAL_QUORUM.",
      defaultValue = "LOCAL_QUORUM")
  public DefaultConsistencyLevel consistencyLevel = DefaultConsistencyLevel.LOCAL_QUORUM;

  @Option(
      names = "--import-max-errors",
      paramLabel = "NUM",
      description =
          "The maximum number of failed records to tolerate when importing data. The default is 1000. "
              + "Failed records will appear in a load.bad file inside the DSBulk operation directory.",
      defaultValue = "1000")
  public int maxErrors = 1000;

  @Option(
      names = "--import-max-concurrent-files",
      paramLabel = "NUM|AUTO",
      description =
          "The maximum number of concurrent files to read from. "
              + "Must be a positive number or the special value AUTO. The default is AUTO.",
      defaultValue = "AUTO")
  public String maxConcurrentFiles = "AUTO";

  @Option(
      names = "--import-max-concurrent-queries",
      paramLabel = "NUM|AUTO",
      description =
          "The maximum number of concurrent queries to execute. "
              + "Must be a positive number or the special value AUTO. The default is AUTO.",
      defaultValue = "AUTO")
  public String maxConcurrentQueries = "AUTO";

  @Option(
      names = "--import-default-timestamp",
      description =
          "The default timestamp to use when importing data. "
              + "Must be a valid instant in ISO-8601 syntax. "
              + "The default is 1970-01-01T00:00:00Z.",
      defaultValue = "1970-01-01T00:00:00Z")
  public Instant defaultTimestamp = Instant.EPOCH;

  @Option(
      names = "--import-dsbulk-option",
      paramLabel = "OPT=VALUE",
      description =
          "An extra DSBulk option to use when importing. "
              + "Any valid DSBulk option can be specified here, and it will passed as is to the DSBulk process. "
              + "DSBulk options, including driver options, must be passed as '--long.option.name=<value>'. "
              + "Short options are not supported. ")
  public List<String> extraDsbulkOptions = new ArrayList<>();

  public long getDefaultTimestampMicros() {
    return ChronoUnit.MICROS.between(Instant.EPOCH, defaultTimestamp);
  }
}
