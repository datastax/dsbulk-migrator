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
package com.datastax.cloudgate.migrator;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public class MigrationSettings {

  public enum TableType {
    REGULAR,
    COUNTER,
    BOTH
  }

  // General settings

  private Path dataDir = Paths.get("export").normalize().toAbsolutePath();
  private Pattern keyspaces = Pattern.compile("^(?!system|dse|OpsCenter)\\w+$");
  private Pattern tables = Pattern.compile(".*");
  private int maxConcurrentOps = 1;
  private TableType tableType = TableType.BOTH;
  private boolean checkTruncateOk = true;

  // Export settings

  private HostAndPort exportHostAndPort = HostAndPort.fromParts("127.0.0.1", 9042);
  private InetSocketAddress exportHostAddress =
      InetSocketAddress.createUnresolved("127.0.0.1", 9042);
  private Path exportBundle;
  private String exportUsername;
  private String exportPassword;
  private int exportMaxRecords = 500000;
  private String exportMaxConcurrentFiles = "AUTO";
  private String exportMaxConcurrentQueries = "AUTO";
  private String exportSplits = "8C";
  private ConsistencyLevel exportConsistency = ConsistencyLevel.LOCAL_QUORUM;

  // Import settings

  private HostAndPort importHostAndPort = HostAndPort.fromParts("127.0.0.1", 9042);
  private InetSocketAddress importHostAddress =
      InetSocketAddress.createUnresolved("127.0.0.1", 9042);
  private Path importBundle;
  private String importUsername;
  private String importPassword;
  private String importMaxConcurrentFiles = "AUTO";
  private String importMaxConcurrentQueries = "AUTO";
  private ConsistencyLevel importConsistency = ConsistencyLevel.LOCAL_QUORUM;
  private long importDefaultTimestamp = 0;

  // DSBulk settings

  private boolean dsbulkEmbedded = false;
  private String dsbulkCmd = "dsbulk";
  private Path dsbulkLogDir = Paths.get("logs").normalize().toAbsolutePath();
  private Path dsbulkWorkingDir;

  // Internal settings

  public MigrationSettings(List<String> args) throws IOException {

    Iterator<String> it = args.iterator();
    while (it.hasNext()) {
      String arg = it.next();

      switch (arg) {
        case "--dataDir":
          dataDir = Paths.get(it.next()).normalize().toAbsolutePath();
          break;
        case "--keyspaces":
          keyspaces = Pattern.compile(it.next());
          break;
        case "--tables":
          tables = Pattern.compile(it.next());
          break;
        case "--tableTypes":
          tableType = TableType.valueOf(it.next());
          break;
        case "--checkTruncateOk":
          checkTruncateOk = Boolean.parseBoolean(it.next());
          break;
        case "--maxConcurrentOps":
          maxConcurrentOps = Integer.parseInt(it.next());
          if (maxConcurrentOps < 1) {
            throw new IllegalArgumentException("Invalid maxConcurrentOps: " + maxConcurrentOps);
          }
          break;

        case "--export.host":
          exportHostAndPort = HostAndPort.fromString(it.next());
          exportHostAddress =
              InetSocketAddress.createUnresolved(
                  exportHostAndPort.getHost(), exportHostAndPort.getPortOrDefault(9042));
          break;
        case "--export.bundle":
          exportBundle = Paths.get(it.next()).normalize().toAbsolutePath();
          break;
        case "--export.username":
          exportUsername = it.next();
          break;
        case "--export.password":
          exportPassword = it.next();
          break;
        case "--export.maxRecords":
          exportMaxRecords = Integer.parseInt(it.next());
          break;
        case "--export.maxConcurrentFiles":
          exportMaxConcurrentFiles = it.next();
          break;
        case "--export.maxConcurrentQueries":
          exportMaxConcurrentQueries = it.next();
          break;
        case "--export.splits":
          exportSplits = it.next();
          break;
        case "--export.consistency":
          exportConsistency = DefaultConsistencyLevel.valueOf(it.next());
          break;

        case "--import.host":
          importHostAndPort = HostAndPort.fromString(it.next());
          importHostAddress =
              InetSocketAddress.createUnresolved(
                  importHostAndPort.getHost(), importHostAndPort.getPortOrDefault(9042));
          break;
        case "--import.bundle":
          importBundle = Paths.get(it.next()).normalize().toAbsolutePath();
          break;
        case "--import.username":
          importUsername = it.next();
          break;
        case "--import.password":
          importPassword = it.next();
          break;
        case "--import.maxConcurrentFiles":
          importMaxConcurrentFiles = it.next();
          break;
        case "--import.maxConcurrentQueries":
          importMaxConcurrentQueries = it.next();
          break;
        case "--import.consistency":
          importConsistency = DefaultConsistencyLevel.valueOf(it.next());
          break;
        case "--import.timestamp":
          importDefaultTimestamp = Long.parseLong(it.next());
          break;

        case "--dsbulk.embedded":
          dsbulkEmbedded = Boolean.parseBoolean(it.next());
          break;
        case "--dsbulk.cmd":
          dsbulkCmd = it.next();
          break;
        case "--dsbulk.logs":
          dsbulkLogDir = Paths.get(it.next()).normalize().toAbsolutePath();
          break;
        case "--dsbulk.workingDir":
          dsbulkWorkingDir = Paths.get(it.next()).normalize().toAbsolutePath();
          break;

        default:
          throw new IllegalArgumentException("Unknown parameter: " + arg);
      }
    }
    checkDirectory(dataDir, "--export.dir");
    checkDirectory(dsbulkLogDir, "--dsbulk.logs");
    checkFile(exportBundle);
    checkFile(importBundle);
  }

  private void checkDirectory(Path dir, String arg) {
    if (Files.exists(dir) && !Files.isDirectory(dir)) {
      throw new IllegalArgumentException("Parameter " + arg + ": is not a directory: " + dir);
    } else if (Files.exists(dir) && !Files.isWritable(dir)) {
      throw new IllegalArgumentException(
          "Parameter " + arg + ": directory is not writable: " + dir);
    }
  }

  private void checkFile(Path file) {
    if (file != null && !Files.isReadable(file)) {
      throw new IllegalArgumentException("File is not readable: " + file);
    }
  }

  public Path getDataDir() {
    return dataDir;
  }

  public Pattern getKeyspaces() {
    return keyspaces;
  }

  public Pattern getTables() {
    return tables;
  }

  public TableType getTableType() {
    return tableType;
  }

  public boolean isCheckTruncateOk() {
    return checkTruncateOk;
  }

  public int getMaxConcurrentOps() {
    return maxConcurrentOps;
  }

  public String getExportHostString() {
    return exportHostAndPort.toString();
  }

  public InetSocketAddress getExportHostAddress() {
    return exportHostAddress;
  }

  public Optional<Path> getExportBundle() {
    return Optional.ofNullable(exportBundle);
  }

  public Optional<String> getExportUsername() {
    return Optional.ofNullable(exportUsername);
  }

  public Optional<String> getExportPassword() {
    return Optional.ofNullable(exportPassword);
  }

  public int getExportMaxRecords() {
    return exportMaxRecords;
  }

  public String getExportMaxConcurrentFiles() {
    return exportMaxConcurrentFiles;
  }

  public String getExportMaxConcurrentQueries() {
    return exportMaxConcurrentQueries;
  }

  public String getExportSplits() {
    return exportSplits;
  }

  public ConsistencyLevel getExportConsistency() {
    return exportConsistency;
  }

  public String getImportHostString() {
    return importHostAndPort.toString();
  }

  public InetSocketAddress getImportHostAddress() {
    return importHostAddress;
  }

  public Optional<Path> getImportBundle() {
    return Optional.ofNullable(importBundle);
  }

  public Optional<String> getImportUsername() {
    return Optional.ofNullable(importUsername);
  }

  public Optional<String> getImportPassword() {
    return Optional.ofNullable(importPassword);
  }

  public String getImportMaxConcurrentFiles() {
    return importMaxConcurrentFiles;
  }

  public String getImportMaxConcurrentQueries() {
    return importMaxConcurrentQueries;
  }

  public ConsistencyLevel getImportConsistency() {
    return importConsistency;
  }

  public long getImportDefaultTimestamp() {
    return importDefaultTimestamp;
  }

  public Path getDsbulkLogDir() {
    return dsbulkLogDir;
  }

  public boolean isDsbulkEmbedded() {
    return dsbulkEmbedded;
  }

  public String getDsbulkCmd() {
    return dsbulkCmd;
  }

  public Optional<Path> getDsbulkWorkingDir() {
    return Optional.ofNullable(dsbulkWorkingDir);
  }
}
