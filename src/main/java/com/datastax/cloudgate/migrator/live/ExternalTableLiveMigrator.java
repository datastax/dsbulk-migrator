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

import com.datastax.cloudgate.migrator.ExportedColumn;
import com.datastax.cloudgate.migrator.MigrationSettings;
import com.datastax.cloudgate.migrator.TableUtils;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalTableLiveMigrator extends TableLiveMigrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalTableLiveMigrator.class);

  public ExternalTableLiveMigrator(
      TableMetadata table, MigrationSettings settings, List<ExportedColumn> exportedColumns) {
    super(table, settings, exportedColumns);
  }

  @Override
  public TableMigrationReport exportTable() {
    String operationId;
    if ((operationId = checkAlreadyExported()) != null) {
      return new TableMigrationReport(this, ExitStatus.STATUS_OK, operationId, true);
    } else {
      LOGGER.info("Exporting {}...", TableUtils.getFullyQualifiedTableName(table));
      operationId = createOperationId(true);
      ExitStatus status = invokeExternalDsbulk(createExportArgs(operationId));
      LOGGER.info(
          "Export of {} finished with {}", TableUtils.getFullyQualifiedTableName(table), status);
      if (status == ExitStatus.STATUS_OK) {
        createExportAckFile(operationId);
        if (TableUtils.isCounterTable(table)) {
          truncateTable();
        }
      }
      return new TableMigrationReport(this, status, operationId, true);
    }
  }

  @Override
  public TableMigrationReport importTable() {
    String operationId;
    if ((operationId = checkAlreadyImported()) != null) {
      return new TableMigrationReport(this, ExitStatus.STATUS_OK, operationId, false);
    } else if (!isAlreadyExported()) {
      throw new IllegalStateException(
          "Cannot import non-exported table: " + TableUtils.getFullyQualifiedTableName(table));
    } else {
      LOGGER.info("Importing {}...", TableUtils.getFullyQualifiedTableName(table));
      operationId = createOperationId(false);
      ExitStatus status = invokeExternalDsbulk(createImportArgs(operationId));
      LOGGER.info(
          "Import of {} finished with {}", TableUtils.getFullyQualifiedTableName(table), status);
      if (status == ExitStatus.STATUS_OK) {
        createImportAckFile(operationId);
      }
      return new TableMigrationReport(this, status, operationId, false);
    }
  }

  private ExitStatus invokeExternalDsbulk(List<String> args) {
    try {
      ProcessBuilder builder = new ProcessBuilder();
      args.add(0, settings.getDsbulkCmd());
      builder.command(args);
      settings.getDsbulkWorkingDir().ifPresent(dir -> builder.directory(dir.toFile()));
      builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
      builder.redirectError(ProcessBuilder.Redirect.DISCARD);
      Process process = builder.start();
      LOGGER.debug(
          "Table {}: process started (PID {})",
          TableUtils.getFullyQualifiedTableName(table),
          process.pid());
      int exitCode = process.waitFor();
      LOGGER.debug(
          "Table {}: process finished (PID {}, exit code {})",
          TableUtils.getFullyQualifiedTableName(table),
          process.pid(),
          exitCode);
      return ExitStatus.forCode(exitCode);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return ExitStatus.STATUS_INTERRUPTED;
    } catch (Exception e) {
      LOGGER.error("DSBulk invocation failed: {}", e.getMessage());
      return ExitStatus.STATUS_CRASHED;
    }
  }
}
