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
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import java.util.List;

public class EmbeddedTableLiveMigrator extends TableLiveMigrator {

  public EmbeddedTableLiveMigrator(
      TableMetadata table, MigrationSettings settings, List<ExportedColumn> exportedColumns) {
    super(table, settings, exportedColumns);
  }

  @Override
  protected ExitStatus invokeDsbulk(List<String> args) {
    DataStaxBulkLoader loader = new DataStaxBulkLoader(args.toArray(new String[0]));
    return ExitStatus.forCode(loader.run().exitCode());
  }
}
