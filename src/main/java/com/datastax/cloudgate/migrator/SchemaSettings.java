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
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class SchemaSettings {

  // Import settings

  private HostAndPort exportHostAndPort = HostAndPort.fromParts("127.0.0.1", 9042);
  private InetSocketAddress exportHostAddress =
      InetSocketAddress.createUnresolved("127.0.0.1", 9042);
  private Path exportBundle;
  private String exportUsername;
  private String exportPassword;
  private Path exportDir = Paths.get("export").normalize().toAbsolutePath();
  private int exportMaxRecords = 500000;
  private String exportMaxConcurrentFiles = "AUTO";
  private String exportMaxConcurrentQueries = "AUTO";
  private String exportSplits = "8C";
  private ConsistencyLevel exportConsistency = ConsistencyLevel.LOCAL_QUORUM;
  private List<CqlIdentifier> exportKeyspaces = new ArrayList<>();

  // Export settings

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

  private String dsbulkCmd = "dsbulk";
  private Path dsbulkLogDir = Paths.get("export").normalize().toAbsolutePath();

  public SchemaSettings(String[] args) throws IOException {

    Iterator<String> it = Arrays.asList(args).iterator();
    while (it.hasNext()) {
      String arg = it.next();

      switch (arg) {
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
        case "--export.dir":
          exportDir = Paths.get(it.next()).normalize().toAbsolutePath();
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
        case "--export.keyspaces":
          exportKeyspaces =
              Splitter.on(",").trimResults().splitToList(it.next()).stream()
                  .map(CqlIdentifier::fromInternal)
                  .collect(Collectors.toList());
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

        case "--dsbulk.cmd":
          dsbulkCmd = it.next();
          break;
        case "--dsbulk.logs":
          dsbulkLogDir = Paths.get(it.next()).normalize().toAbsolutePath();
          break;

        default:
          throw new IllegalArgumentException("Unknown parameter: " + arg);
      }
    }
    checkDirectory(exportDir, "--export.dir");
    checkDirectory(dsbulkLogDir, "--dsbulk.logs");
    checkFile(exportBundle);
    checkFile(importBundle);
  }

  private void checkDirectory(Path dir, String arg) throws IOException {
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

  public Path getExportDir() {
    return exportDir;
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

  public List<CqlIdentifier> getExportKeyspaces() {
    return exportKeyspaces;
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

  public String getDsbulkCmd() {
    return dsbulkCmd;
  }
}
