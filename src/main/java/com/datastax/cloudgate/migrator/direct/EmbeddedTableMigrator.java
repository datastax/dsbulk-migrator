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
package com.datastax.cloudgate.migrator.direct;

import com.datastax.cloudgate.migrator.ExportedColumn;
import com.datastax.cloudgate.migrator.MigrationSettings;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedTableMigrator extends TableMigrator {

  public static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedTableMigrator.class);

  public EmbeddedTableMigrator(
      TableMetadata table, MigrationSettings settings, List<ExportedColumn> exportedColumns) {
    super(table, settings, exportedColumns);
  }

  @Override
  public TableMigrationReport exportTable() {
    String operationId;
    if ((operationId = checkAlreadyExported()) != null) {
      return new TableMigrationReport(this, ExitStatus.STATUS_OK, operationId, true);
    } else {
      LOGGER.info("Exporting {}...", getFullyQualifiedTableName());
      operationId = createOperationId(true);
      String[] args = createExportArgs(operationId).toArray(new String[0]);
      ExitStatus status = ExitStatus.forCode(new DataStaxBulkLoader(args).run().exitCode());
      LOGGER.info("Export of {} finished with {}", getFullyQualifiedTableName(), status);
      if (status == ExitStatus.STATUS_OK) {
        createExportAckFile(operationId);
      }
      return new TableMigrationReport(this, status, operationId, true);
    }
  }

  @Override
  public TableMigrationReport importTable() {
    String operationId;
    if ((operationId = checkAlreadyImported()) != null) {
      return new TableMigrationReport(this, ExitStatus.STATUS_OK, operationId, false);
    } else if (checkNotYetExported()) {
      return new TableMigrationReport(this, ExitStatus.STATUS_ABORTED_FATAL_ERROR, null, false);
    } else {
      LOGGER.info("Importing {}...", getFullyQualifiedTableName());
      operationId = createOperationId(false);
      String[] args = createImportArgs(operationId).toArray(new String[0]);
      ExitStatus status = ExitStatus.forCode(new DataStaxBulkLoader(args).run().exitCode());
      LOGGER.info("Import of {} finished with {}", getFullyQualifiedTableName(), status);
      if (status == ExitStatus.STATUS_OK) {
        createImportAckFile(operationId);
      }
      return new TableMigrationReport(this, status, operationId, false);
    }
  }
}
