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

import com.datastax.cloudgate.migrator.MigrationSettings;
import com.datastax.cloudgate.migrator.TableUtils;
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

  public void generate() throws IOException {
    Path exportDir = settings.getDataDir();
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
  }

  private void printExportScriptHeader(PrintWriter writer) {
    writer.println("#!/bin/bash");
    writer.println();
    writer.println(
        "bundle=\"${MIGRATOR_EXPORT_BUNDLE:-"
            + settings.getExportBundle().map(Object::toString).orElse("")
            + "}\"");
    writer.println("host=\"${MIGRATOR_EXPORT_HOST:-" + settings.getExportHostString() + "}\"");
    writer.println(
        "username=\"${MIGRATOR_EXPORT_USERNAME:-"
            + settings.getExportUsername().map(Object::toString).orElse("")
            + "}\"");
    writer.println(
        "password=\"${MIGRATOR_EXPORT_PASSWORD:-"
            + settings.getExportPassword().map(Object::toString).orElse("")
            + "}\"");
    writer.println("dsbulk_cmd=\"${MIGRATOR_EXPORT_CMD:-" + settings.getDsbulkCmd() + "}\"");
    writer.println(
        "dsbulk_logs=\"${MIGRATOR_EXPORT_LOG_DIR:-" + settings.getDsbulkLogDir() + "}\"");
    writer.println("data_dir=\"${MIGRATOR_DATA_DIR:-" + settings.getDataDir() + "}\"");
    writer.println(
        "max_records=\"${MIGRATOR_EXPORT_MAX_RECORDS:-" + settings.getExportMaxRecords() + "}\"");
    writer.println(
        "max_concurrent_files=\"${MIGRATOR_EXPORT_MAX_CONCURRENT_FILES:-"
            + settings.getExportMaxConcurrentFiles()
            + "}\"");
    writer.println(
        "max_concurrent_queries=\"${MIGRATOR_EXPORT_MAX_CONCURRENT_QUERIES:-"
            + settings.getExportMaxConcurrentQueries()
            + "}\"");
    writer.println("splits=\"${MIGRATOR_EXPORT_SPLITS:-" + settings.getExportSplits() + "}\"");
    writer.println(
        "consistency_level=\"${MIGRATOR_EXPORT_CONSISTENCY:-"
            + settings.getExportConsistency()
            + "}\"");
    writer.println();
    writer.flush();
  }

  private void printImportScriptHeader(PrintWriter writer, boolean counter) {
    writer.println("#!/bin/bash");
    writer.println();
    writer.println(
        "bundle=\"${MIGRATOR_IMPORT_BUNDLE:-"
            + settings.getImportBundle().map(Object::toString).orElse("")
            + "}\"");
    writer.println("host=\"${MIGRATOR_IMPORT_HOST:-" + settings.getImportHostString() + "}\"");
    writer.println(
        "username=\"${MIGRATOR_IMPORT_USERNAME:-"
            + settings.getImportUsername().map(Object::toString).orElse("")
            + "}\"");
    writer.println(
        "password=\"${MIGRATOR_IMPORT_PASSWORD:-"
            + settings.getImportPassword().map(Object::toString).orElse("")
            + "}\"");
    writer.println("dsbulk_cmd=\"${MIGRATOR_IMPORT_CMD:-" + settings.getDsbulkCmd() + "}\"");
    writer.println(
        "dsbulk_logs=\"${MIGRATOR_IMPORT_LOG_DIR:-" + settings.getDsbulkLogDir() + "}\"");
    writer.println("data_dir=\"${MIGRATOR_DATA_DIR:-" + settings.getDataDir() + "}\"");
    writer.println(
        "max_concurrent_files=\"${MIGRATOR_IMPORT_MAX_CONCURRENT_FILES:-"
            + settings.getImportMaxConcurrentFiles()
            + "}\"");
    writer.println(
        "max_concurrent_queries=\"${MIGRATOR_IMPORT_MAX_CONCURRENT_QUERIES:-"
            + settings.getImportMaxConcurrentQueries()
            + "}\"");
    writer.println(
        "max_errors=\"${MIGRATOR_IMPORT_MAX_ERRORS:-" + settings.getImportMaxErrors() + "}\"");
    writer.println(
        "consistency_level=\"${MIGRATOR_IMPORT_CONSISTENCY:-"
            + settings.getImportConsistency()
            + "}\"");
    if (!counter) {
      writer.println(
          "default_writetime=\"${MIGRATOR_IMPORT_DEFAULT_WRITETIME:-"
              + settings.getImportDefaultTimestamp()
              + "}\"");
    }
    writer.println();
    writer.flush();
  }
}
