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
package com.datastax.cloudgate.migrator.ddl;

import com.datastax.cloudgate.migrator.live.ExitStatus;
import com.datastax.cloudgate.migrator.live.SchemaLiveMigrator;
import com.datastax.cloudgate.migrator.model.ExportedTable;
import com.datastax.cloudgate.migrator.utils.ModelUtils;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaDdlGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLiveMigrator.class);

  private static final CqlIdentifier DEFAULT_TIME_TO_LIVE =
      CqlIdentifier.fromInternal("default_time_to_live");

  private final Set<KeyspaceMetadata> keyspaces = new LinkedHashSet<>();
  private final Set<FunctionMetadata> functions = new LinkedHashSet<>();
  private final Set<AggregateMetadata> aggregates = new LinkedHashSet<>();
  private final Set<UserDefinedType> udts = new LinkedHashSet<>();
  private final Set<TableMetadata> tables = new LinkedHashSet<>();
  private final Set<IndexMetadata> indexes = new LinkedHashSet<>();

  private final DdlGenerationSettings settings;

  public SchemaDdlGenerator(DdlGenerationSettings settings) {
    this.settings = settings;
    List<ExportedTable> exportedTables =
        ModelUtils.buildExportedTables(
            settings.clusterInfo, settings.credentials, settings.tlsSettings, settings);
    for (ExportedTable exportedTable : exportedTables) {
      keyspaces.add(exportedTable.keyspace);
      // include all functions, aggregates and user-defined types
      exportedTable.keyspace.getFunctions().forEach((id, function) -> functions.add(function));
      exportedTable.keyspace.getAggregates().forEach((id, aggregate) -> aggregates.add(aggregate));
      exportedTable.keyspace.getUserDefinedTypes().forEach((id, udt) -> udts.add(udt));
      tables.add(settings.optimizeForAstra ? applyAstraLimits(exportedTable) : exportedTable.table);
      // only include indexes targeting an exported table
      exportedTable.table.getIndexes().forEach((id, index) -> indexes.add(index));
    }
  }

  public ExitStatus generate() throws IOException {
    Path exportDir = settings.dataDir;
    Files.createDirectories(exportDir);

    Path keyspacesFile = exportDir.resolve("cloud-gate-migrator-ddl-keyspaces.cql");
    Path tablesFile = exportDir.resolve("cloud-gate-migrator-ddl-tables.cql");
    Path functionsFile = exportDir.resolve("cloud-gate-migrator-ddl-functions.cql");
    Path aggregatesFile = exportDir.resolve("cloud-gate-migrator-ddl-aggregates.cql");
    Path udtsFile = exportDir.resolve("cloud-gate-migrator-ddl-udts.cql");
    Path indexesFile = exportDir.resolve("cloud-gate-migrator-ddl-indexes.cql");

    try (PrintWriter keyspacesWriter =
        new PrintWriter(Files.newBufferedWriter(keyspacesFile, StandardCharsets.UTF_8))) {
      for (KeyspaceMetadata keyspace : keyspaces) {
        keyspacesWriter.println(keyspace.describe(false));
      }
      LOGGER.info("Successfully generated CREATE KEYSPACE file: {}", keyspacesFile);
    }

    try (PrintWriter tablesWriter =
        new PrintWriter(Files.newBufferedWriter(tablesFile, StandardCharsets.UTF_8))) {
      for (TableMetadata table : tables) {
        tablesWriter.println(table.describe(false));
      }
      LOGGER.info("Successfully generated CREATE TABLE file: {}", tablesFile);
    }

    if (!functions.isEmpty()) {
      try (PrintWriter functionsWriter =
          new PrintWriter(Files.newBufferedWriter(functionsFile, StandardCharsets.UTF_8))) {
        for (FunctionMetadata function : functions) {
          functionsWriter.println(function.describe(false));
        }
        LOGGER.info("Successfully generated CREATE FUNCTION file: {}", functionsFile);
      }
    }

    if (!aggregates.isEmpty()) {
      try (PrintWriter aggregatesWriter =
          new PrintWriter(Files.newBufferedWriter(aggregatesFile, StandardCharsets.UTF_8))) {
        for (AggregateMetadata aggregate : aggregates) {
          aggregatesWriter.println(aggregate.describe(false));
        }
        LOGGER.info("Successfully generated CREATE AGGREGATE file: {}", aggregatesFile);
      }
    }

    if (!udts.isEmpty()) {
      try (PrintWriter udtsWriter =
          new PrintWriter(Files.newBufferedWriter(udtsFile, StandardCharsets.UTF_8))) {
        for (UserDefinedType udt : udts) {
          udtsWriter.println(udt.describe(false));
        }
        LOGGER.info("Successfully generated CREATE TYPE file: {}", udtsFile);
      }
    }

    if (!indexes.isEmpty()) {
      try (PrintWriter indexesWriter =
          new PrintWriter(Files.newBufferedWriter(indexesFile, StandardCharsets.UTF_8))) {
        for (IndexMetadata index : indexes) {
          indexesWriter.println(index.describe(false));
        }
        LOGGER.info("Successfully generated CREATE INDEX file: {}", indexesFile);
      }
    }
    return ExitStatus.STATUS_OK;
  }

  private static TableMetadata applyAstraLimits(ExportedTable exportedTable) {
    TableMetadata table = exportedTable.table;
    if (table.isVirtual()) {
      throw new IllegalStateException(
          "Cannot generate CREATE TABLE statement for a virtual table: "
              + exportedTable.fullyQualifiedName);
    }
    if (table.isCompactStorage()) {
      LOGGER.warn(
          "Skipping COMPACT STORAGE option for table: {}", exportedTable.fullyQualifiedName);
    }
    Map<CqlIdentifier, Object> astraOptions =
        table.getOptions().entrySet().stream()
            .filter(entry -> entry.getKey().equals(DEFAULT_TIME_TO_LIVE))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
    return new DefaultTableMetadata(
        table.getKeyspace(),
        table.getName(),
        table.getId().orElse(null),
        false,
        false,
        table.getPartitionKey(),
        table.getClusteringColumns(),
        table.getColumns(),
        astraOptions,
        table.getIndexes());
  }
}
