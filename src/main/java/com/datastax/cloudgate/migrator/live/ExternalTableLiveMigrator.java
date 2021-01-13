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
  protected ExitStatus invokeDsbulk(List<String> args) {
    try {
      ProcessBuilder builder = new ProcessBuilder();
      args.add(0, String.valueOf(settings.dsBulkSettings.dsbulkCmd));
      builder.command(args);
      if (settings.dsBulkSettings.dsbulkWorkingDir != null) {
        builder.directory(settings.dsBulkSettings.dsbulkWorkingDir.toFile());
      }
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
