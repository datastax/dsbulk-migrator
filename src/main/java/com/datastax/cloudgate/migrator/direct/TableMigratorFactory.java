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
import com.datastax.cloudgate.migrator.TableProcessorFactory;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.util.List;

public class TableMigratorFactory extends TableProcessorFactory<TableMigrator> {

  @Override
  protected TableMigrator create(
      TableMetadata table, MigrationSettings settings, List<ExportedColumn> exportedColumns) {
    if (settings.isDsbulkEmbedded()) {
      return new EmbeddedTableMigrator(table, settings, exportedColumns);
    } else {
      return new ExternalTableMigrator(table, settings, exportedColumns);
    }
  }
}