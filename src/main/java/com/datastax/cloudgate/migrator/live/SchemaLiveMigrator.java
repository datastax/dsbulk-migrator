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

import com.datastax.cloudgate.migrator.utils.ModelUtils;
import com.datastax.cloudgate.migrator.utils.SessionUtils;
import com.datastax.oss.driver.api.core.CqlSession;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLiveMigrator.class);

  private static final Object POISON_PILL = new Object();

  private final LiveMigrationSettings settings;
  private final BlockingQueue<TableLiveMigrator> exportQueue;
  private final BlockingQueue<Object> importQueue;
  private final ExecutorService pool;

  private final CopyOnWriteArrayList<TableMigrationReport> successful =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<TableMigrationReport> failed = new CopyOnWriteArrayList<>();

  private final List<TableLiveMigrator> migrators;
  private final boolean hasRegularTables;
  private final boolean hasCounterTables;

  public SchemaLiveMigrator(LiveMigrationSettings settings) {
    this.settings = settings;
    if (settings.dsbulkEmbedded) {
      checkEmbeddedDSBulkAvailable();
    }
    exportQueue = new LinkedBlockingQueue<>();
    importQueue = new LinkedBlockingQueue<>();
    pool =
        settings.maxConcurrentOps == 1
            ? MoreExecutors.newDirectExecutorService()
            : Executors.newFixedThreadPool(settings.maxConcurrentOps);
    migrators =
        ModelUtils.buildExportedTables(
                settings.exportSettings.clusterInfo, settings.exportSettings.credentials, settings)
            .stream()
            .map(
                exportedTable -> {
                  if (settings.dsbulkEmbedded) {
                    return new EmbeddedTableLiveMigrator(exportedTable, settings);
                  } else {
                    return new ExternalTableLiveMigrator(exportedTable, settings);
                  }
                })
            .collect(Collectors.toList());
    hasRegularTables =
        migrators.stream().anyMatch(migrator -> !migrator.getExportedTable().counterTable);
    hasCounterTables =
        migrators.stream().anyMatch(migrator -> migrator.getExportedTable().counterTable);
  }

  private void checkEmbeddedDSBulkAvailable() {
    try {
      Class.forName("com.datastax.oss.dsbulk.runner.DataStaxBulkLoader");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "DSBulk is not available on the classpath; cannot use embedded mode.");
    }
  }

  public ExitStatus migrate() {
    try {
      // Origin cluster connectivity has already been tested by TableProcessorFactory
      testTargetClusterConnectivity();
      askPermissionToTruncate();
      if (hasRegularTables) {
        LOGGER.info("Migrating regular tables...");
        migrateTables(migrator -> !migrator.getExportedTable().counterTable);
      }
      exportQueue.clear();
      importQueue.clear();
      if (hasCounterTables) {
        LOGGER.info("Migrating counter tables...");
        migrateTables(migrator -> migrator.getExportedTable().counterTable);
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
        LOGGER.info("Table {} migrated successfully.", report.getMigrator().getExportedTable());
      }
      for (TableMigrationReport report : failed) {
        LOGGER.error(
            "Table {} could not be {}: operation {} exited with {}.",
            report.getMigrator().getExportedTable(),
            report.isExport() ? "exported" : "imported",
            report.getOperationId(),
            report.getStatus());
      }
    }
    if (failed.isEmpty()) {
      return ExitStatus.STATUS_OK;
    }
    if (successful.isEmpty()) {
      return ExitStatus.STATUS_ABORTED_TOO_MANY_ERRORS;
    }
    return ExitStatus.STATUS_COMPLETED_WITH_ERRORS;
  }

  @SuppressWarnings("EmptyTryBlock")
  private void testTargetClusterConnectivity() {
    try (CqlSession ignored =
        SessionUtils.createSession(
            settings.importSettings.clusterInfo, settings.importSettings.credentials)) {}
  }

  private void migrateTables(Predicate<TableLiveMigrator> filter) {
    List<TableLiveMigrator> filtered =
        migrators.stream().filter(filter).collect(Collectors.toList());
    exportQueue.addAll(filtered);
    List<CompletableFuture<?>> futures = new ArrayList<>();
    for (int i = 0; i < settings.maxConcurrentOps; i++) {
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
                + migrator.getExportedTable()
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
                + migrator.getExportedTable()
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

  private void askPermissionToTruncate() {
    if (hasCounterTables && !settings.skipTruncateConfirmation) {
      List<TableLiveMigrator> remainingCounterTables =
          migrators.stream()
              .filter(migrator -> migrator.getExportedTable().counterTable)
              .filter(migrator -> !migrator.isImported())
              .collect(Collectors.toList());
      if (!remainingCounterTables.isEmpty()) {
        // Bypass the logging system and hit System.err directly
        System.err.println("WARNING!");
        System.err.println(
            (settings.truncateBeforeExport ? "Before" : "After")
                + " they are exported, counter tables must be truncated on the target cluster.");
        System.err.println(
            "If you agree to proceed, the following tables WILL BE TRUNCATED on the TARGET cluster:");
        remainingCounterTables.forEach(
            migrator -> System.err.printf("- %s%n", migrator.getExportedTable()));
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
                .filter(migrator -> migrator.getExportedTable().counterTable)
                .forEach(
                    migrator ->
                        failed.add(
                            new TableMigrationReport(
                                migrator,
                                ExitStatus.STATUS_ABORTED_FATAL_ERROR,
                                null,
                                settings.truncateBeforeExport)));
            throw new CancellationException("Truncate permission denied by user");
          }
        }
      }
    }
  }
}
