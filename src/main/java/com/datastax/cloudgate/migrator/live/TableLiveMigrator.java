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

import com.datastax.cloudgate.migrator.processor.ExportedColumn;
import com.datastax.cloudgate.migrator.processor.TableProcessor;
import com.datastax.cloudgate.migrator.settings.MigrationSettings;
import com.datastax.cloudgate.migrator.utils.TableUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableLiveMigrator extends TableProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableLiveMigrator.class);

  protected final Path dataDir;

  protected final Path exportAckDir;
  protected final Path exportAckFile;

  protected final Path importAckDir;
  protected final Path importAckFile;

  public TableLiveMigrator(
      TableMetadata table, MigrationSettings settings, List<ExportedColumn> exportedColumns) {
    super(table, settings, exportedColumns);
    this.dataDir =
        settings
            .generalSettings
            .dataDir
            .resolve(table.getKeyspace().asInternal())
            .resolve(table.getName().asInternal());
    this.exportAckDir = settings.generalSettings.dataDir.resolve("__exported__");
    this.importAckDir = settings.generalSettings.dataDir.resolve("__imported__");
    this.exportAckFile =
        exportAckDir.resolve(
            table.getKeyspace().asInternal() + "__" + table.getName().asInternal() + ".exported");
    this.importAckFile =
        importAckDir.resolve(
            table.getKeyspace().asInternal() + "__" + table.getName().asInternal() + ".imported");
  }

  public abstract TableMigrationReport exportTable();

  public abstract TableMigrationReport importTable();

  protected String createOperationId(boolean export) {
    ZonedDateTime now = Instant.now().atZone(ZoneOffset.UTC);
    String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").format(now);
    return String.format(
        "%s_%s_%s_%s",
        (export ? "EXPORT" : "IMPORT"),
        table.getKeyspace().asInternal(),
        table.getName().asInternal(),
        timestamp);
  }

  public boolean isAlreadyExported() {
    return Files.exists(exportAckFile);
  }

  public boolean isAlreadyImported() {
    return Files.exists(importAckFile);
  }

  protected String checkAlreadyExported() {
    if (isAlreadyExported()) {
      try {
        String operationId = Files.readString(exportAckFile);
        if (operationId != null && !operationId.isBlank()) {
          LOGGER.warn(
              "Table {}.{}: already exported, skipping (delete this file to re-export: {}).",
              table.getKeyspace(),
              table.getName(),
              exportAckFile);
          return operationId;
        }
      } catch (IOException ignored) {
      }
    }
    return null;
  }

  protected String checkAlreadyImported() {
    if (isAlreadyImported()) {
      try {
        String operationId = Files.readString(importAckFile);
        if (operationId != null && !operationId.isBlank()) {
          LOGGER.warn(
              "Table {}.{}: already imported, skipping (delete this file to re-import: {}).",
              table.getKeyspace(),
              table.getName(),
              importAckFile);
          return operationId;
        }
      } catch (IOException ignored) {
      }
    }
    return null;
  }

  protected void createExportAckFile(String operationId) {
    try {
      Files.createDirectories(exportAckDir);
      Files.createFile(exportAckFile);
      Files.write(exportAckFile, operationId.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected void createImportAckFile(String operationId) {
    try {
      Files.createDirectories(importAckDir);
      Files.createFile(importAckFile);
      Files.write(importAckFile, operationId.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected List<String> createExportArgs(String operationId) {
    List<String> args = new ArrayList<>();
    args.add("unload");
    if (settings.exportSettings.clusterInfo.bundle != null) {
      args.add("-b");
      args.add(String.valueOf(settings.exportSettings.clusterInfo.bundle));
    } else {
      args.add("-h");
      args.add("[\"" + settings.exportSettings.clusterInfo.hostAndPort + "\"]");
    }
    if (settings.exportSettings.credentials != null) {
      args.add("-u");
      args.add(settings.exportSettings.credentials.username);
      args.add("-p");
      args.add(String.valueOf(settings.exportSettings.credentials.password));
    }
    args.add("-url");
    args.add(String.valueOf(dataDir));
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
    args.add("-verbosity");
    args.add("0");
    args.add("--engine.executionId");
    args.add(operationId);
    args.add("-logDir");
    args.add(String.valueOf(settings.dsBulkSettings.dsbulkLogDir));
    args.add("-query");
    args.add(buildExportQuery());
    return args;
  }

  protected List<String> createImportArgs(String operationId) {
    List<String> args = new ArrayList<>();
    args.add("load");
    if (settings.importSettings.clusterInfo.bundle != null) {
      args.add("-b");
      args.add(String.valueOf(settings.importSettings.clusterInfo.bundle));
    } else {
      args.add("-h");
      args.add("[\"" + settings.importSettings.clusterInfo.hostAndPort + "\"]");
    }
    if (settings.importSettings.credentials != null) {
      args.add("-u");
      args.add(settings.importSettings.credentials.username);
      args.add("-p");
      args.add(String.valueOf(settings.importSettings.credentials.password));
    }
    args.add("-url");
    args.add(String.valueOf(dataDir));
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
    args.add("-verbosity");
    args.add("0");
    args.add("--engine.executionId");
    args.add(operationId);
    args.add("-logDir");
    args.add(String.valueOf(settings.dsBulkSettings.dsbulkLogDir));
    args.add("-m");
    args.add(buildImportMapping());
    int regularColumns = countRegularColumns();
    if (regularColumns == 0) {
      args.add("-k");
      args.add(escape(table.getKeyspace()));
      args.add("-t");
      args.add(escape(table.getName()));
    } else if (regularColumns == 1) {
      args.add("-query");
      args.add(buildSingleImportQuery());
    } else {
      args.add("--batch.mode");
      args.add("DISABLED");
      args.add("-query");
      args.add(buildBatchImportQuery());
    }
    return args;
  }

  protected void truncateTable() {
    String tableName = TableUtils.getFullyQualifiedTableName(table);
    LOGGER.info("Truncating {} on target cluster...", tableName);
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, "DcInferringLoadBalancingPolicy")
            .build();
    CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(loader);
    if (settings.importSettings.clusterInfo.bundle != null) {
      builder.withCloudSecureConnectBundle(settings.importSettings.clusterInfo.bundle);
    } else {
      builder.addContactPoint(settings.importSettings.clusterInfo.getHostAddress());
    }
    if (settings.importSettings.credentials != null) {
      builder.withAuthCredentials(
          settings.importSettings.credentials.username,
          String.valueOf(settings.importSettings.credentials.password));
    }
    try (CqlSession session = builder.build()) {
      session.execute("TRUNCATE " + tableName);
      LOGGER.info("Successfully truncated {} on target cluster", tableName);
    }
  }

  protected String escape(String text) {
    return text.replace("\"", "\\\"");
  }

  protected String getImportDefaultTimestamp() {
    return String.valueOf(settings.importSettings.getDefaultTimestampMicros());
  }
}
