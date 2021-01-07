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
package com.datastax.cloudgate.migrator.script;

import com.datastax.cloudgate.migrator.live.ExitStatus;
import com.datastax.cloudgate.migrator.settings.MigrationSettings;
import com.datastax.cloudgate.migrator.utils.TableUtils;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaScriptGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaScriptGenerator.class);

  private final List<TableScriptGenerator> generators;
  private final MigrationSettings settings;
  private final boolean hasRegularTables;
  private final boolean hasCounterTables;

  public SchemaScriptGenerator(MigrationSettings settings) {
    this.settings = settings;
    generators = new TableScriptGeneratorFactory().create(settings);
    hasRegularTables =
        generators.stream().anyMatch(gen -> !TableUtils.isCounterTable(gen.getTable()));
    hasCounterTables =
        generators.stream().anyMatch(gen -> TableUtils.isCounterTable(gen.getTable()));
  }

  public ExitStatus generate() throws IOException {
    Path exportDir = settings.generalSettings.dataDir;
    Files.createDirectories(exportDir);
    Path exportScript = exportDir.resolve("cloud-gate-migrator-export.sh");
    Path importScript = exportDir.resolve("cloud-gate-migrator-import.sh");
    if (hasRegularTables) {
      try (PrintWriter exportWriter =
              new PrintWriter(Files.newBufferedWriter(exportScript, StandardCharsets.UTF_8));
          PrintWriter importWriter =
              new PrintWriter(Files.newBufferedWriter(importScript, StandardCharsets.UTF_8))) {
        printExportScriptHeader(exportWriter);
        printImportScriptHeader(importWriter, false);
        for (TableScriptGenerator generator : generators) {
          if (!TableUtils.isCounterTable(generator.getTable())) {
            generator.printExportScript(exportWriter);
            generator.printImportScript(importWriter);
          }
        }
      }
    }
    Path exportScriptCounters = exportDir.resolve("cloud-gate-migrator-export-counters.sh");
    Path importScriptCounters = exportDir.resolve("cloud-gate-migrator-import-counters.sh");
    if (hasCounterTables) {
      try (PrintWriter exportWriter =
              new PrintWriter(
                  Files.newBufferedWriter(exportScriptCounters, StandardCharsets.UTF_8));
          PrintWriter importWriter =
              new PrintWriter(
                  Files.newBufferedWriter(importScriptCounters, StandardCharsets.UTF_8))) {
        printExportScriptHeader(exportWriter);
        printImportScriptHeader(importWriter, true);
        for (TableScriptGenerator generator : generators) {
          if (TableUtils.isCounterTable(generator.getTable())) {
            generator.printExportScript(exportWriter);
            generator.printImportScript(importWriter);
          }
        }
      }
    }
    LOGGER.info("Scripts successfully generated:");
    if (hasRegularTables) {
      LOGGER.info("Export script: {}", exportScript);
      LOGGER.info("Import script: {}", importScript);
    }
    if (hasCounterTables) {
      LOGGER.info("Export script (counter tables): {}", exportScriptCounters);
      LOGGER.info("Import script (counter tables): {}", importScriptCounters);
    }
    return ExitStatus.STATUS_OK;
  }

  private void printExportScriptHeader(PrintWriter writer) {
    writer.println("#!/bin/bash");
    writer.println();
    writer.println(
        "bundle=\"${MIGRATOR_EXPORT_BUNDLE:-"
            + (settings.exportSettings.clusterInfo.bundle != null
                ? settings.exportSettings.clusterInfo.bundle
                : "")
            + "}\"");
    writer.println(
        "host=\"${MIGRATOR_EXPORT_HOST:-"
            + (settings.exportSettings.clusterInfo.hostAndPort != null
                ? settings.exportSettings.clusterInfo.hostAndPort
                : "")
            + "}\"");
    writer.println(
        "username=\"${MIGRATOR_EXPORT_USERNAME:-"
            + (settings.exportSettings.credentials != null
                ? settings.exportSettings.credentials.username
                : "")
            + "}\"");
    writer.println(
        "password=\"${MIGRATOR_EXPORT_PASSWORD:-"
            + (settings.exportSettings.credentials != null
                ? String.valueOf(settings.exportSettings.credentials.password)
                : "")
            + "}\"");
    writer.println(
        "dsbulk_cmd=\"${MIGRATOR_EXPORT_CMD:-" + settings.dsBulkSettings.dsbulkCmd + "}\"");
    writer.println(
        "dsbulk_logs=\"${MIGRATOR_EXPORT_LOG_DIR:-" + settings.dsBulkSettings.dsbulkLogDir + "}\"");
    writer.println(
        "data_dir=\"${MIGRATOR_EXPORT_DATA_DIR:-" + settings.generalSettings.dataDir + "}\"");
    writer.println(
        "max_records=\"${MIGRATOR_EXPORT_MAX_RECORDS:-"
            + settings.exportSettings.maxRecords
            + "}\"");
    writer.println(
        "max_concurrent_files=\"${MIGRATOR_EXPORT_MAX_CONCURRENT_FILES:-"
            + settings.exportSettings.maxConcurrentFiles
            + "}\"");
    writer.println(
        "max_concurrent_queries=\"${MIGRATOR_EXPORT_MAX_CONCURRENT_QUERIES:-"
            + settings.exportSettings.maxConcurrentQueries
            + "}\"");
    writer.println("splits=\"${MIGRATOR_EXPORT_SPLITS:-" + settings.exportSettings.splits + "}\"");
    writer.println(
        "consistency_level=\"${MIGRATOR_EXPORT_CONSISTENCY:-"
            + settings.exportSettings.consistencyLevel
            + "}\"");
    writer.println();
    writer.flush();
  }

  private void printImportScriptHeader(PrintWriter writer, boolean counter) {
    writer.println("#!/bin/bash");
    writer.println();
    writer.println(
        "bundle=\"${MIGRATOR_IMPORT_BUNDLE:-"
            + (settings.importSettings.clusterInfo.bundle != null
                ? settings.importSettings.clusterInfo.bundle
                : "")
            + "}\"");
    writer.println(
        "host=\"${MIGRATOR_IMPORT_HOST:-"
            + (settings.importSettings.clusterInfo.hostAndPort != null
                ? settings.importSettings.clusterInfo.hostAndPort
                : "")
            + "}\"");
    writer.println(
        "username=\"${MIGRATOR_IMPORT_USERNAME:-"
            + (settings.importSettings.credentials != null
                ? settings.importSettings.credentials.username
                : "")
            + "}\"");
    writer.println(
        "password=\"${MIGRATOR_IMPORT_PASSWORD:-"
            + (settings.importSettings.credentials != null
                ? String.valueOf(settings.importSettings.credentials.password)
                : "")
            + "}\"");
    writer.println(
        "dsbulk_cmd=\"${MIGRATOR_IMPORT_CMD:-" + settings.dsBulkSettings.dsbulkCmd + "}\"");
    writer.println(
        "dsbulk_logs=\"${MIGRATOR_IMPORT_LOG_DIR:-" + settings.dsBulkSettings.dsbulkLogDir + "}\"");
    writer.println(
        "data_dir=\"${MIGRATOR_IMPORT_DATA_DIR:-" + settings.generalSettings.dataDir + "}\"");
    writer.println(
        "max_concurrent_files=\"${MIGRATOR_IMPORT_MAX_CONCURRENT_FILES:-"
            + settings.importSettings.maxConcurrentFiles
            + "}\"");
    writer.println(
        "max_concurrent_queries=\"${MIGRATOR_IMPORT_MAX_CONCURRENT_QUERIES:-"
            + settings.importSettings.maxConcurrentQueries
            + "}\"");
    writer.println(
        "max_errors=\"${MIGRATOR_IMPORT_MAX_ERRORS:-" + settings.importSettings.maxErrors + "}\"");
    writer.println(
        "consistency_level=\"${MIGRATOR_IMPORT_CONSISTENCY:-"
            + settings.importSettings.consistencyLevel
            + "}\"");
    if (!counter) {
      writer.println(
          "default_writetime=\"${MIGRATOR_IMPORT_DEFAULT_WRITETIME:-"
              + settings.importSettings.getDefaultTimestampMicros()
              + "}\"");
    }
    writer.println();
    writer.flush();
  }
}
