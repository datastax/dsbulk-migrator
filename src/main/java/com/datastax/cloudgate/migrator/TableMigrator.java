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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.ExitStatus;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableMigrator extends TableProcessor {

  public static final Logger LOGGER = LoggerFactory.getLogger(TableMigrator.class);

  protected final Path exportDir;

  public TableMigrator(
      TableMetadata table, SchemaSettings settings, List<ExportedColumn> exportedColumns) {
    super(table, settings, exportedColumns);
    this.exportDir =
        settings
            .getExportDir()
            .resolve(table.getKeyspace().asInternal())
            .resolve(table.getName().asInternal());
  }

  public ExitStatus exportTable() {
    LOGGER.info("Exporting {}.{}...", table.getKeyspace(), table.getName());
    ExitStatus status = new DataStaxBulkLoader(createExportArgs()).run();
    LOGGER.info("Export of {}.{} finished with {}", table.getKeyspace(), table.getName(), status);
    return status;
  }

  public ExitStatus importTable() {
    LOGGER.info("Importing {}.{}...", table.getKeyspace(), table.getName());
    ExitStatus status = new DataStaxBulkLoader(createImportArgs()).run();
    LOGGER.info("Import of {}.{} finished with {}", table.getKeyspace(), table.getName(), status);
    return status;
  }

  private String[] createExportArgs() {
    List<String> args = new ArrayList<>();
    args.add("unload");
    if (settings.getExportBundle().isPresent()) {
      args.add("-b");
      args.add(String.valueOf(settings.getImportBundle()));
    } else {
      args.add("-h");
      args.add("[\"" + settings.getExportHostString() + "\"]");
    }
    if (settings.getExportUsername().isPresent()) {
      args.add("-u");
      args.add(settings.getExportUsername().get());
    }
    if (settings.getExportPassword().isPresent()) {
      args.add("-p");
      args.add(settings.getExportPassword().get());
    }
    args.add("-url");
    args.add(String.valueOf(exportDir));
    args.add("-maxRecords");
    args.add(String.valueOf(settings.getExportMaxRecords()));
    args.add("-maxConcurrentFiles");
    args.add(settings.getExportMaxConcurrentFiles());
    args.add("-maxConcurrentQueries");
    args.add(settings.getExportMaxConcurrentQueries());
    args.add("--schema.splits");
    args.add(settings.getExportSplits());
    args.add("-cl");
    args.add(String.valueOf(settings.getExportConsistency()));
    args.add("-header");
    args.add("false");
    args.add("--monitoring.console");
    args.add("false");
    args.add("--engine.executionId");
    args.add(buildOperationId("EXPORT"));
    args.add("-logDir");
    args.add(String.valueOf(settings.getDsbulkLogDir()));
    args.add("-query");
    args.add(buildExportQuery());
    return args.toArray(new String[0]);
  }

  private String[] createImportArgs() {
    List<String> args = new ArrayList<>();
    args.add("load");
    if (settings.getImportBundle().isPresent()) {
      args.add("-b");
      args.add(String.valueOf(settings.getImportBundle()));
    } else {
      args.add("-h");
      args.add("[\"" + settings.getExportHostString() + "\"]");
    }
    if (settings.getImportUsername().isPresent()) {
      args.add("-u");
      args.add(settings.getImportUsername().get());
    }
    if (settings.getImportPassword().isPresent()) {
      args.add("-p");
      args.add(settings.getImportPassword().get());
    }
    args.add("-url");
    args.add(String.valueOf(exportDir));
    args.add("-maxConcurrentFiles");
    args.add(settings.getImportMaxConcurrentFiles());
    args.add("-maxConcurrentQueries");
    args.add(settings.getImportMaxConcurrentQueries());
    args.add("-cl");
    args.add(String.valueOf(settings.getImportConsistency()));
    args.add("-header");
    args.add("false");
    args.add("--monitoring.console");
    args.add("false");
    args.add("--engine.executionId");
    args.add(buildOperationId("IMPORT"));
    args.add("-logDir");
    args.add(String.valueOf(settings.getDsbulkLogDir()));
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
    return args.toArray(new String[0]);
  }

  protected String escape(CqlIdentifier id) {
    return id.asCql(true).replace("\"", "\\\"");
  }

  protected String getImportDefaultTimestamp() {
    return String.valueOf(settings.getImportDefaultTimestamp());
  }

  private String buildOperationId(String prefix) {
    return prefix
        + "_"
        + table.getKeyspace().asInternal()
        + "_"
        + table.getName().asInternal()
        + "_"
        + "%2$tY"
        + "%2$tm"
        + "%2$td"
        + "_"
        + "%2$tH"
        + "%2$tM"
        + "%2$tS"
        + "_"
        + "%2$tL";
  }
}
