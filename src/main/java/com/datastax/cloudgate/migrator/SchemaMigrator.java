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

import ch.qos.logback.classic.Level;
import com.datastax.oss.dsbulk.runner.ExitStatus;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.*;
import org.slf4j.LoggerFactory;

public class SchemaMigrator {

  private static final Object POISON_PILL = new Object();

  public static void main(String[] args) throws IOException {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.datastax.cloudgate");
    logger.setLevel(Level.INFO);
    SchemaMigrator migrator = new SchemaMigrator(args);
    migrator.migrate();
  }

  private final Queue<TableMigrator> exportQueue;
  private final BlockingQueue<TableMigrator> importQueue;
  private final ExecutorService exporters;
  private final ExecutorService importers;

  public SchemaMigrator(String[] args) throws IOException {
    SchemaSettings settings = new SchemaSettings(args);
    exportQueue = new LinkedBlockingQueue<>(new TableMigratorFactory().create(settings));
    importQueue = new LinkedBlockingQueue<>();
    exporters = Executors.newFixedThreadPool(1);
    importers = Executors.newFixedThreadPool(1);
  }

  public void migrate() {
    try {
      // TODO parallelize
      CompletableFuture<?> exportFuture = CompletableFuture.runAsync(this::exportTables, exporters);
      exportFuture.join();
      CompletableFuture<?> importFuture = CompletableFuture.runAsync(this::importTables, importers);
      importFuture.join();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      exporters.shutdownNow();
      importers.shutdownNow();
    }
  }

  private void exportTables() throws UnexpectedExitStatusException {
    TableMigrator migration;
    while ((migration = exportQueue.poll()) != null) {
      ExitStatus status = migration.exportTable();
      if (status == ExitStatus.STATUS_OK) {
        importQueue.add(migration);
      } else {
        throw new UnexpectedExitStatusException(status);
      }
    }
    importQueue.add((TableMigrator) POISON_PILL);
  }

  private void importTables() throws UnexpectedExitStatusException {
    while (true) {
      TableMigrator migration;
      try {
        migration = importQueue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (migration == POISON_PILL) {
        break;
      }
      ExitStatus status = migration.importTable();
      if (status != ExitStatus.STATUS_OK) {
        throw new UnexpectedExitStatusException(status);
      }
    }
  }
}
