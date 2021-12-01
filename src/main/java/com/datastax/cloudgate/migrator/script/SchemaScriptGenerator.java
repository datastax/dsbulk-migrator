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
import com.datastax.cloudgate.migrator.utils.ModelUtils;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaScriptGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaScriptGenerator.class);

  private final List<TableScriptGenerator> generators;
  private final ScriptGenerationSettings settings;
  private final boolean hasRegularTables;
  private final boolean hasCounterTables;

  public SchemaScriptGenerator(ScriptGenerationSettings settings) {
    this.settings = settings;
    generators =
        ModelUtils.buildExportedTables(
                settings.exportSettings.clusterInfo,
                settings.exportSettings.credentials,
                settings.exportSettings.tlsSettings,
                settings)
            .stream()
            .map(exportedTable -> new TableScriptGenerator(exportedTable, settings))
            .collect(Collectors.toList());
    hasRegularTables = generators.stream().anyMatch(gen -> !gen.getExportedTable().counterTable);
    hasCounterTables = generators.stream().anyMatch(gen -> gen.getExportedTable().counterTable);
  }

  public ExitStatus generate() throws IOException {
    Path exportDir = settings.dataDir;
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
          if (!generator.getExportedTable().counterTable) {
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
          if (generator.getExportedTable().counterTable) {
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
    if (settings.exportSettings.clusterInfo.isAstra()) {
      writer.println(
          "bundle=\"${MIGRATOR_EXPORT_BUNDLE:-"
              + (settings.exportSettings.clusterInfo.bundle != null
                  ? settings.exportSettings.clusterInfo.bundle
                  : "")
              + "}\"");
    } else {
      String hosts = "";
      if (settings.exportSettings.clusterInfo.hostsAndPorts != null) {
        hosts =
            "["
                + settings.exportSettings.clusterInfo.hostsAndPorts.stream()
                    .map(hp -> "\\\"" + hp + "\\\"")
                    .collect(Collectors.joining(","))
                + "]";
      }
      writer.println("hosts=\"${MIGRATOR_EXPORT_HOSTS:-" + hosts + "}\"");
    }
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
    writer.println("dsbulk_cmd=\"${MIGRATOR_EXPORT_CMD:-" + settings.dsbulkCmd + "}\"");
    writer.println("dsbulk_logs=\"${MIGRATOR_EXPORT_LOG_DIR:-" + settings.dsbulkLogDir + "}\"");
    writer.println("data_dir=\"${MIGRATOR_EXPORT_DATA_DIR:-" + settings.dataDir + "}\"");
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
    if (settings.importSettings.clusterInfo.isAstra()) {
      writer.println(
          "bundle=\"${MIGRATOR_IMPORT_BUNDLE:-"
              + (settings.importSettings.clusterInfo.bundle != null
                  ? settings.importSettings.clusterInfo.bundle
                  : "")
              + "}\"");
    } else {
      String hosts = "";
      if (settings.importSettings.clusterInfo.hostsAndPorts != null) {
        hosts =
            "["
                + settings.importSettings.clusterInfo.hostsAndPorts.stream()
                    .map(hp -> "\\\"" + hp + "\\\"")
                    .collect(Collectors.joining(","))
                + "]";
      }
      writer.println("hosts=\"${MIGRATOR_IMPORT_HOSTS:-" + hosts + "}\"");
    }
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
    writer.println("dsbulk_cmd=\"${MIGRATOR_IMPORT_CMD:-" + settings.dsbulkCmd + "}\"");
    writer.println("dsbulk_logs=\"${MIGRATOR_IMPORT_LOG_DIR:-" + settings.dsbulkLogDir + "}\"");
    writer.println("data_dir=\"${MIGRATOR_IMPORT_DATA_DIR:-" + settings.dataDir + "}\"");
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
