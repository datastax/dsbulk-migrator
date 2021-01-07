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
import com.datastax.cloudgate.migrator.settings.MigrationSettings;
import com.datastax.cloudgate.migrator.utils.TableUtils;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedTableLiveMigrator extends TableLiveMigrator {

  public static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedTableLiveMigrator.class);

  public EmbeddedTableLiveMigrator(
      TableMetadata table, MigrationSettings settings, List<ExportedColumn> exportedColumns) {
    super(table, settings, exportedColumns);
  }

  @Override
  public TableMigrationReport exportTable() {
    String operationId;
    if ((operationId = checkAlreadyExported()) != null) {
      return new TableMigrationReport(this, ExitStatus.STATUS_OK, operationId, true);
    } else {
      if (settings.generalSettings.truncateBeforeExport && TableUtils.isCounterTable(table)) {
        truncateTable();
      }
      LOGGER.info("Exporting {}...", TableUtils.getFullyQualifiedTableName(table));
      operationId = createOperationId(true);
      String[] args = createExportArgs(operationId).toArray(new String[0]);
      ExitStatus status = ExitStatus.forCode(new DataStaxBulkLoader(args).run().exitCode());
      LOGGER.info(
          "Export of {} finished with {}", TableUtils.getFullyQualifiedTableName(table), status);
      if (status == ExitStatus.STATUS_OK) {
        createExportAckFile(operationId);
        if (!settings.generalSettings.truncateBeforeExport && TableUtils.isCounterTable(table)) {
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
      String[] args = createImportArgs(operationId).toArray(new String[0]);
      ExitStatus status = ExitStatus.forCode(new DataStaxBulkLoader(args).run().exitCode());
      LOGGER.info(
          "Import of {} finished with {}", TableUtils.getFullyQualifiedTableName(table), status);
      if (status == ExitStatus.STATUS_OK) {
        createImportAckFile(operationId);
      }
      return new TableMigrationReport(this, status, operationId, false);
    }
  }
}
