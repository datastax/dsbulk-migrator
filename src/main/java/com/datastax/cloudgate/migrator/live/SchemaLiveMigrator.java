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

import com.datastax.cloudgate.migrator.MigrationSettings;
import com.datastax.cloudgate.migrator.TableUtils;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaLiveMigrator {

  public static final Logger LOGGER = LoggerFactory.getLogger(SchemaLiveMigrator.class);

  private static final Object POISON_PILL = new Object();

  private final MigrationSettings settings;
  private final BlockingQueue<TableLiveMigrator> exportQueue;
  private final BlockingQueue<Object> importQueue;
  private final ExecutorService pool;

  private final CopyOnWriteArrayList<TableMigrationReport> successful =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<TableMigrationReport> failed = new CopyOnWriteArrayList<>();

  private final List<TableLiveMigrator> migrators;
  private final boolean hasRegularTables;
  private final boolean hasCounterTables;

  public SchemaLiveMigrator(MigrationSettings settings) {
    this.settings = settings;
    exportQueue = new LinkedBlockingQueue<>();
    importQueue = new LinkedBlockingQueue<>();
    pool =
        settings.getMaxConcurrentOps() == 1
            ? MoreExecutors.newDirectExecutorService()
            : Executors.newFixedThreadPool(settings.getMaxConcurrentOps());
    migrators = new TableMigratorFactory().create(settings);
    hasRegularTables =
        migrators.stream().anyMatch(migrator -> !TableUtils.isCounterTable(migrator.getTable()));
    hasCounterTables =
        migrators.stream().anyMatch(migrator -> TableUtils.isCounterTable(migrator.getTable()));
  }

  public void migrate() {
    try {
      checkTruncateOk();
      if (hasRegularTables) {
        LOGGER.info("Migrating regular tables...");
        migrateTables(migrator -> !TableUtils.isCounterTable(migrator.getTable()));
      }
      exportQueue.clear();
      importQueue.clear();
      if (hasCounterTables) {
        LOGGER.info("Migrating counter tables...");
        migrateTables(migrator -> TableUtils.isCounterTable(migrator.getTable()));
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    } finally {
      pool.shutdownNow();
      LOGGER.info(
          "Migration finished with {} successfully migrated tables, {} failed tables.",
          successful.size(),
          failed.size());
      for (TableMigrationReport report : successful) {
        LOGGER.info(
            "Table {} migrated successfully.",
            TableUtils.getFullyQualifiedTableName(report.getMigrator().getTable()));
      }
      for (TableMigrationReport report : failed) {
        LOGGER.error(
            "Table {} could not be {}: operation {} exited with {}.",
            TableUtils.getFullyQualifiedTableName(report.getMigrator().getTable()),
            report.isExport() ? "exported" : "imported",
            report.getOperationId(),
            report.getStatus());
      }
    }
  }

  private void migrateTables(Predicate<TableLiveMigrator> filter) {
    List<TableLiveMigrator> filtered =
        migrators.stream().filter(filter).collect(Collectors.toList());
    exportQueue.addAll(filtered);
    List<CompletableFuture<?>> futures = new ArrayList<>();
    for (int i = 0; i < settings.getMaxConcurrentOps(); i++) {
      futures.add(CompletableFuture.runAsync(this::exportTables, pool));
      futures.add(CompletableFuture.runAsync(this::importTables, pool));
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
  }

  private void exportTables() {
    TableLiveMigrator migrator;
    while ((migrator = exportQueue.poll()) != null) {
      TableMigrationReport report;
      try {
        report = migrator.exportTable();
      } catch (Exception e) {
        LOGGER.error(
            "Table "
                + TableUtils.getFullyQualifiedTableName(migrator.getTable())
                + ": unexpected error when exporting data, aborting",
            e);
        report =
            new TableMigrationReport(migrator, ExitStatus.STATUS_ABORTED_FATAL_ERROR, null, true);
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
      TableLiveMigrator migrator;
      try {
        Object o = importQueue.take();
        if (o == POISON_PILL) {
          break;
        }
        migrator = (TableLiveMigrator) o;
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
                + TableUtils.getFullyQualifiedTableName(migrator.getTable())
                + ": unexpected error when importing data, aborting",
            e);
        report =
            new TableMigrationReport(migrator, ExitStatus.STATUS_ABORTED_FATAL_ERROR, null, false);
      }
      if (report.getStatus() == ExitStatus.STATUS_OK) {
        successful.add(report);
      } else {
        failed.add(report);
      }
    }
  }

  private void checkTruncateOk() {
    if (hasCounterTables && settings.isCheckTruncateOk()) {
      List<TableLiveMigrator> remainingCounterTables =
          migrators.stream()
              .filter(migrator -> TableUtils.isCounterTable(migrator.getTable()))
              .filter(migrator -> !migrator.isAlreadyImported())
              .collect(Collectors.toList());
      if (!remainingCounterTables.isEmpty()) {
        // Bypass the logging system and hit System.err directly
        System.err.println("WARNING!");
        System.err.println(
            (settings.isTruncateAfterExport() ? "After" : "Before")
                + " they are exported, counter tables must be truncated on the target cluster.");
        System.err.println(
            "If you agree to proceed, the following tables WILL BE TRUNCATED on the TARGET cluster:");
        remainingCounterTables.forEach(
            migrator ->
                System.err.printf(
                    "- %s%n", TableUtils.getFullyQualifiedTableName(migrator.getTable())));
        System.err.println(
            "Note that the above tables will NOT be truncated on the origin cluster.");
        while (true) {
          System.err.println("Are you OK proceeding with the migration? (y/N)");
          Scanner scanner = new Scanner(System.in);
          String input = scanner.nextLine();
          if (input.equalsIgnoreCase("y")) {
            return;
          } else if (input.isBlank() || input.equalsIgnoreCase("n")) {
            migrators.stream()
                .filter(migrator -> TableUtils.isCounterTable(migrator.getTable()))
                .forEach(
                    migrator ->
                        failed.add(
                            new TableMigrationReport(
                                migrator, ExitStatus.STATUS_ABORTED_FATAL_ERROR, null, false)));
            throw new CancellationException("Truncate permission denied by user");
          }
        }
      }
    }
  }
}
