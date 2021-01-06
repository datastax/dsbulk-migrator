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

import com.datastax.cloudgate.migrator.MigrationSettings;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaMigrator {

  public static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigrator.class);

  private static final Object POISON_PILL = new Object();

  private final MigrationSettings settings;
  private final BlockingQueue<TableMigrator> exportQueue;
  private final BlockingQueue<Object> importQueue;
  private final ExecutorService pool;

  private final CopyOnWriteArrayList<TableMigrationReport> successful =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<TableMigrationReport> failed = new CopyOnWriteArrayList<>();

  public SchemaMigrator(MigrationSettings settings) {
    this.settings = settings;
    exportQueue = new LinkedBlockingQueue<>();
    importQueue = new LinkedBlockingQueue<>();
    pool =
        settings.getMaxConcurrentOps() == 1
            ? MoreExecutors.newDirectExecutorService()
            : Executors.newFixedThreadPool(settings.getMaxConcurrentOps());
  }

  public void migrate() {
    List<TableMigrator> migrators = new TableMigratorFactory().create(settings);
    exportQueue.addAll(migrators);
    try {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      for (int i = 0; i < settings.getMaxConcurrentOps(); i++) {
        futures.add(CompletableFuture.runAsync(this::exportTables, pool));
        futures.add(CompletableFuture.runAsync(this::importTables, pool));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    } finally {
      pool.shutdownNow();
    }
    LOGGER.info(
        "Migration finished with {} successfully migrated tables, {} failed tables.",
        successful.size(),
        failed.size());
    for (TableMigrationReport report : successful) {
      LOGGER.info(
          "Table {} migrated successfully.", report.getMigrator().getFullyQualifiedTableName());
    }
    for (TableMigrationReport report : failed) {
      LOGGER.error(
          "Table {} could not be {}: operation {} exited with {}.",
          report.getMigrator().getFullyQualifiedTableName(),
          report.isExport() ? "exported" : "imported",
          report.getOperationId(),
          report.getStatus());
    }
  }

  private void exportTables() {
    TableMigrator migrator;
    while ((migrator = exportQueue.poll()) != null) {
      TableMigrationReport report;
      try {
        report = migrator.exportTable();
      } catch (Exception e) {
        LOGGER.error(
            "Table "
                + migrator.getFullyQualifiedTableName()
                + ": unexpected error when exporting data, aborting",
            e);
        break;
      }
      if (report.getStatus() == ExitStatus.STATUS_OK) {
        importQueue.add(migrator);
      } else {
        failed.add(report);
      }
    }
    importQueue.add(POISON_PILL);
  }

  private void importTables() {
    while (true) {
      TableMigrator migrator;
      try {
        Object o = importQueue.take();
        if (o == POISON_PILL) {
          break;
        }
        migrator = (TableMigrator) o;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      TableMigrationReport report;
      try {
        report = migrator.importTable();
      } catch (Exception e) {
        LOGGER.error(
            "Table "
                + migrator.getFullyQualifiedTableName()
                + ": unexpected error when importing data, aborting",
            e);
        break;
      }
      if (report.getStatus() == ExitStatus.STATUS_OK) {
        successful.add(report);
      } else {
        failed.add(report);
      }
    }
  }
}
