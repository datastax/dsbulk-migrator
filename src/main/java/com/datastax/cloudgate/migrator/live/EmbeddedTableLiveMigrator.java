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

import com.datastax.cloudgate.migrator.model.ExportedTable;
import com.datastax.cloudgate.migrator.utils.LoggingUtils;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import java.net.URL;
import java.util.List;
import java.util.Objects;

public class EmbeddedTableLiveMigrator extends TableLiveMigrator {

  private static final URL DSBULK_CONFIGURATION_FILE =
      Objects.requireNonNull(ClassLoader.getSystemResource("logback-dsbulk-embedded.xml"));

  public EmbeddedTableLiveMigrator(ExportedTable exportedTable, LiveMigrationSettings settings) {
    super(exportedTable, settings);
  }

  @Override
  protected ExitStatus invokeDsbulk(String operationId, List<String> args) {
    DataStaxBulkLoader loader = new DataStaxBulkLoader(args.toArray(new String[0]));
    int exitCode;
    LoggingUtils.configureLogging(DSBULK_CONFIGURATION_FILE);
    System.setProperty("OPERATION_ID", operationId);
    try {
      exitCode = loader.run().exitCode();
    } finally {
      System.clearProperty("OPERATION_ID");
      LoggingUtils.configureLogging(LoggingUtils.MIGRATOR_CONFIGURATION_FILE);
    }
    return ExitStatus.forCode(exitCode);
  }
}
