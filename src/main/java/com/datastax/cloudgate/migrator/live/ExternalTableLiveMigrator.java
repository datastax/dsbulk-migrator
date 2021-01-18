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
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalTableLiveMigrator extends TableLiveMigrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalTableLiveMigrator.class);

  public ExternalTableLiveMigrator(ExportedTable exportedTable, LiveMigrationSettings settings) {
    super(exportedTable, settings);
  }

  @Override
  protected ExitStatus invokeDsbulk(List<String> args) {
    try {
      ProcessBuilder builder = new ProcessBuilder();
      args.add(0, String.valueOf(settings.dsbulkCmd));
      builder.command(args);
      if (settings.dsbulkWorkingDir != null) {
        builder.directory(settings.dsbulkWorkingDir.toFile());
      }
      Process process = builder.start();
      LOGGER.debug(
          "Table {}: process started (PID {})", exportedTable.fullyQualifiedName, process.pid());
      ExecutorService pool = Executors.newFixedThreadPool(2);
      ExitStatus status;
      try {
        pool.submit(new StreamPiper(process.getInputStream(), System.out::println));
        pool.submit(new StreamPiper(process.getErrorStream(), System.err::println));
        int exitCode = process.waitFor();
        status = ExitStatus.forCode(exitCode);
      } finally {
        pool.shutdown();
        if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
          LOGGER.error("Timed out waiting for process streams to close");
        }
        pool.shutdownNow();
      }
      LOGGER.debug(
          "Table {}: process finished (PID {}, exit code {})",
          exportedTable.fullyQualifiedName,
          process.pid(),
          status);
      return status;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return ExitStatus.STATUS_INTERRUPTED;
    } catch (Exception e) {
      LOGGER.error("DSBulk invocation failed: {}", e.getMessage());
      return ExitStatus.STATUS_CRASHED;
    }
  }

  private static class StreamPiper implements Runnable {

    private final InputStream inputStream;
    private final Consumer<String> consumer;

    StreamPiper(InputStream inputStream, Consumer<String> consumer) {
      this.inputStream = inputStream;
      this.consumer = consumer;
    }

    @Override
    public void run() {
      new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer);
    }
  }
}
