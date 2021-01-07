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
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ImportSettings {

  @ArgGroup(multiplicity = "1")
  public ClusterInfo clusterInfo;

  public static class ClusterInfo {

    @Option(
        names = "--import-host",
        paramLabel = "HOST[:PORT]",
        description =
            "The host name or IP and, optionally, the port of a node from the target cluster. "
                + "If the port is not specified, it will default to 9042. "
                + "Options --export-host and --export-bundle are mutually exclusive.",
        converter = HostAndPortConverter.class,
        required = true)
    public HostAndPort hostAndPort;

    @Option(
        names = "--import-bundle",
        paramLabel = "PATH",
        description =
            "The path to a secure connect bundle to connect to the target cluster, "
                + "if that cluster is a DataStax Astra cluster. "
                + "Options --export-host and --export-bundle are mutually exclusive.",
        required = true)
    public Path bundle;

    public InetSocketAddress getHostAddress() {
      return InetSocketAddress.createUnresolved(
          hostAndPort.getHost(), hostAndPort.hasPort() ? hostAndPort.getPort() : 9042);
    }
  }

  @ArgGroup(exclusive = false)
  public Credentials credentials;

  public static class Credentials {

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

  public long getDefaultTimestampMicros() {
    return ChronoUnit.MICROS.between(Instant.EPOCH, defaultTimestamp);
  }
}
