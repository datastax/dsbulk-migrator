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
import com.datastax.cloudgate.migrator.processor.TableProcessorFactory;
import com.datastax.cloudgate.migrator.settings.MigrationSettings;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.util.List;

public class TableMigratorFactory extends TableProcessorFactory<TableLiveMigrator> {

  @Override
  protected TableLiveMigrator create(
      TableMetadata table, MigrationSettings settings, List<ExportedColumn> exportedColumns) {
    if (settings.dsBulkSettings.dsbulkEmbedded) {
      checkEmbeddedDSBulkAvailable();
      return new EmbeddedTableLiveMigrator(table, settings, exportedColumns);
    } else {
      return new ExternalTableLiveMigrator(table, settings, exportedColumns);
    }
  }

  private void checkEmbeddedDSBulkAvailable() {
    try {
      Class.forName("com.datastax.oss.dsbulk.runner.DataStaxBulkLoader");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "DSBulk is not available on the classpath; cannot use embedded mode.");
    }
  }
}
