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
package com.datastax.cloudgate.migrator.live;

import com.datastax.cloudgate.migrator.model.ExportedTable;
import com.datastax.cloudgate.migrator.model.TableProcessor;
import com.datastax.cloudgate.migrator.utils.SessionUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableLiveMigrator extends TableProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableLiveMigrator.class);

  protected final LiveMigrationSettings settings;

  protected final Path tableDataDir;

  protected final Path exportAckDir;
  protected final Path exportAckFile;

  protected final Path importAckDir;
  protected final Path importAckFile;

  public TableLiveMigrator(ExportedTable exportedTable, LiveMigrationSettings settings) {
    super(exportedTable);
    this.settings = settings;
    this.tableDataDir =
        settings
            .dataDir
            .resolve(exportedTable.keyspace.getName().asInternal())
            .resolve(exportedTable.table.getName().asInternal());
    this.exportAckDir = settings.dataDir.resolve("__exported__");
    this.importAckDir = settings.dataDir.resolve("__imported__");
    this.exportAckFile =
        exportAckDir.resolve(
            exportedTable.keyspace.getName().asInternal()
                + "__"
                + exportedTable.table.getName().asInternal()
                + ".exported");
    this.importAckFile =
        importAckDir.resolve(
            exportedTable.keyspace.getName().asInternal()
                + "__"
                + exportedTable.table.getName().asInternal()
                + ".imported");
  }

  public TableMigrationReport exportTable() {
    String operationId;
    if ((operationId = retrieveExportOperationId()) != null) {
      LOGGER.warn(
          "Table {}.{}: already exported, skipping (delete this file to re-export: {}).",
          exportedTable.keyspace.getName(),
          exportedTable.table.getName(),
          exportAckFile);
      return new TableMigrationReport(this, ExitStatus.STATUS_OK, operationId, true);
    } else {
      if (settings.truncateBeforeExport && exportedTable.counterTable) {
        truncateTable();
      }
      LOGGER.info("Exporting {}...", exportedTable.fullyQualifiedName);
      operationId = createOperationId(true);
      List<String> args = createExportArgs(operationId);
      ExitStatus status = invokeDsbulk(args);
      LOGGER.info("Export of {} finished with {}", exportedTable.fullyQualifiedName, status);
      if (status == ExitStatus.STATUS_OK) {
        createExportAckFile(operationId);
        if (!settings.truncateBeforeExport && exportedTable.counterTable) {
          truncateTable();
        }
      }
      return new TableMigrationReport(this, status, operationId, true);
    }
  }

  public TableMigrationReport importTable() {
    String operationId;
    if ((operationId = retrieveImportOperationId()) != null) {
      LOGGER.warn(
          "Table {}.{}: already imported, skipping (delete this file to re-import: {}).",
          exportedTable.keyspace.getName(),
          exportedTable.table.getName(),
          importAckFile);
      return new TableMigrationReport(this, ExitStatus.STATUS_OK, operationId, false);
    } else if (!isExported()) {
      throw new IllegalStateException(
          "Cannot import non-exported table: " + exportedTable.fullyQualifiedName);
    } else if (!hasExportedData()) {
      LOGGER.warn(
          "Table {}.{}: export did not create any output file(s), skipping import. Is the table empty?",
          exportedTable.keyspace.getName(),
          exportedTable.table.getName());
      operationId = createOperationId(false);
      createImportAckFile(operationId);
      return new TableMigrationReport(this, ExitStatus.STATUS_OK, operationId, false);
    } else {
      LOGGER.info("Importing {}...", exportedTable.fullyQualifiedName);
      operationId = createOperationId(false);
      List<String> args = createImportArgs(operationId);
      ExitStatus status = invokeDsbulk(args);
      LOGGER.info("Import of {} finished with {}", exportedTable.fullyQualifiedName, status);
      if (status == ExitStatus.STATUS_OK) {
        createImportAckFile(operationId);
      }
      return new TableMigrationReport(this, status, operationId, false);
    }
  }

  public boolean isExported() {
    return Files.exists(exportAckFile);
  }

  public boolean isImported() {
    return Files.exists(importAckFile);
  }

  protected abstract ExitStatus invokeDsbulk(List<String> args);

  private String createOperationId(boolean export) {
    ZonedDateTime now = Instant.now().atZone(ZoneOffset.UTC);
    String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").format(now);
    return String.format(
        "%s_%s_%s_%s",
        (export ? "EXPORT" : "IMPORT"),
        exportedTable.keyspace.getName().asInternal(),
        exportedTable.table.getName().asInternal(),
        timestamp);
  }

  private String retrieveExportOperationId() {
    if (isExported()) {
      try {
        String operationId = Files.readString(exportAckFile);
        if (operationId != null && !operationId.isBlank()) {
          return operationId;
        }
      } catch (IOException ignored) {
      }
    }
    return null;
  }

  private String retrieveImportOperationId() {
    if (isImported()) {
      try {
        String operationId = Files.readString(importAckFile);
        if (operationId != null && !operationId.isBlank()) {
          return operationId;
        }
      } catch (IOException ignored) {
      }
    }
    return null;
  }

  private void createExportAckFile(String operationId) {
    try {
      Files.createDirectories(exportAckDir);
      Files.createFile(exportAckFile);
      Files.write(exportAckFile, operationId.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void createImportAckFile(String operationId) {
    try {
      Files.createDirectories(importAckDir);
      Files.createFile(importAckFile);
      Files.write(importAckFile, operationId.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean hasExportedData() {
    if (isExported()) {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableDataDir)) {
        for (Path entry : stream) {
          String fileName = entry.getFileName().toString();
          if (fileName.startsWith("output")) {
            return true;
          }
        }
      } catch (IOException ignored) {
      }
    }
    return false;
  }

  private List<String> createExportArgs(String operationId) {
    List<String> args = new ArrayList<>();
    args.add("unload");
    if (settings.exportSettings.clusterInfo.bundle != null) {
      args.add("-b");
      args.add(String.valueOf(settings.exportSettings.clusterInfo.bundle));
    } else {
      args.add("-h");
      String hosts =
          settings.exportSettings.clusterInfo.hostsAndPorts.stream()
              .map(hp -> "\"" + hp + "\"")
              .collect(Collectors.joining(","));
      args.add("[" + hosts + "]");
    }
    if (settings.exportSettings.credentials != null) {
      args.add("-u");
      args.add(settings.exportSettings.credentials.username);
      args.add("-p");
      args.add(String.valueOf(settings.exportSettings.credentials.password));
    }
    args.add("-url");
    args.add(String.valueOf(tableDataDir));
    args.add("-maxRecords");
    args.add(String.valueOf(settings.exportSettings.maxRecords));
    args.add("-maxConcurrentFiles");
    args.add(settings.exportSettings.maxConcurrentFiles);
    args.add("-maxConcurrentQueries");
    args.add(settings.exportSettings.maxConcurrentQueries);
    args.add("--schema.splits");
    args.add(settings.exportSettings.splits);
    args.add("-cl");
    args.add(String.valueOf(settings.exportSettings.consistencyLevel));
    args.add("-maxErrors");
    args.add("0");
    args.add("-header");
    args.add("false");
    args.add("--engine.executionId");
    args.add(operationId);
    args.add("-logDir");
    args.add(String.valueOf(settings.dsbulkLogDir));
    args.add("-query");
    args.add(buildExportQuery());
    args.addAll(settings.exportSettings.extraDsbulkOptions);
    return args;
  }

  private List<String> createImportArgs(String operationId) {
    List<String> args = new ArrayList<>();
    args.add("load");
    if (settings.importSettings.clusterInfo.bundle != null) {
      args.add("-b");
      args.add(String.valueOf(settings.importSettings.clusterInfo.bundle));
    } else {
      args.add("-h");
      String hosts =
          settings.importSettings.clusterInfo.hostsAndPorts.stream()
              .map(hp -> "\"" + hp + "\"")
              .collect(Collectors.joining(","));
      args.add("[" + hosts + "]");
    }
    if (settings.importSettings.credentials != null) {
      args.add("-u");
      args.add(settings.importSettings.credentials.username);
      args.add("-p");
      args.add(String.valueOf(settings.importSettings.credentials.password));
    }
    args.add("-url");
    args.add(String.valueOf(tableDataDir));
    args.add("-maxConcurrentFiles");
    args.add(settings.importSettings.maxConcurrentFiles);
    args.add("-maxConcurrentQueries");
    args.add(settings.importSettings.maxConcurrentQueries);
    args.add("-cl");
    args.add(String.valueOf(settings.importSettings.consistencyLevel));
    args.add("-maxErrors");
    args.add(String.valueOf(settings.importSettings.maxErrors));
    args.add("-header");
    args.add("false");
    args.add("--engine.executionId");
    args.add(operationId);
    args.add("-logDir");
    args.add(String.valueOf(settings.dsbulkLogDir));
    args.add("-m");
    args.add(buildImportMapping());
    int regularColumns = countRegularColumns();
    if (regularColumns == 0) {
      args.add("-k");
      args.add(escape(exportedTable.keyspace.getName()));
      args.add("-t");
      args.add(escape(exportedTable.table.getName()));
    } else if (regularColumns == 1) {
      args.add("-query");
      args.add(buildSingleImportQuery());
    } else {
      args.add("--batch.mode");
      args.add("DISABLED");
      args.add("-query");
      args.add(buildBatchImportQuery());
    }
    args.addAll(settings.importSettings.extraDsbulkOptions);
    return args;
  }

  private void truncateTable() {
    String tableName = exportedTable.fullyQualifiedName;
    LOGGER.info("Truncating {} on target cluster...", tableName);
    try (CqlSession session =
        SessionUtils.createSession(
            settings.importSettings.clusterInfo, settings.importSettings.credentials)) {
      session.execute("TRUNCATE " + tableName);
      LOGGER.info("Successfully truncated {} on target cluster", tableName);
    }
  }

  @Override
  protected String escape(String text) {
    return text.replace("\"", "\\\"");
  }

  @Override
  protected String getImportDefaultTimestamp() {
    return String.valueOf(settings.importSettings.getDefaultTimestampMicros());
  }
}
