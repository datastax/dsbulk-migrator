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
package com.datastax.migrator.live;

import com.datastax.migrator.model.TableType;
import com.datastax.migrator.settings.ExportSettings;
import com.datastax.migrator.settings.ImportSettings;
import com.datastax.migrator.settings.InclusionSettings;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class LiveMigrationSettings implements InclusionSettings {

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message.")
  boolean usageHelpRequested;

  @Option(
      names = {"-d", "--data-dir"},
      paramLabel = "PATH",
      description =
          "The directory where data will be exported to and imported from."
              + "The default is a 'data' subdirectory in the current working directory. "
              + "The data directory will be created if it does not exist. "
              + "Tables will be exported and imported in subdirectories of the data directory specified here; "
              + "there will be one subdirectory per keyspace inside the data directory, "
              + "then one subdirectory per table inside each keyspace directory.",
      defaultValue = "data")
  public Path dataDir = Paths.get("data");

  @Option(
      names = {"-k", "--keyspaces"},
      paramLabel = "REGEX",
      description =
          "A regular expression to select keyspaces to migrate. "
              + "The default is to migrate all keyspaces except system keyspaces, DSE-specific keyspaces, and the OpsCenter keyspace. "
              + "Case-sensitive keyspace names must be entered in their exact case.",
      defaultValue = "^(?!system|dse|OpsCenter)\\w+$")
  public Pattern keyspacesPattern = Pattern.compile("^(?!system|dse|OpsCenter)\\w+$");

  @Option(
      names = {"-t", "--tables"},
      paramLabel = "REGEX",
      description =
          "A regular expression to select tables to migrate."
              + "The default is to migrate all tables inside the keyspaces that were selected for migration with --keyspaces. "
              + "Case-sensitive table names must be entered in their exact case.",
      defaultValue = ".*")
  public Pattern tablesPattern = Pattern.compile(".*");

  @Option(
      names = "--table-types",
      paramLabel = "regular|counter|all",
      description = "The table types to migrate (regular, counter, or all). Default is all.",
      defaultValue = "all")
  public TableType tableType = TableType.all;

  @ArgGroup(exclusive = false, multiplicity = "1")
  public ExportSettings exportSettings = new ExportSettings();

  @ArgGroup(exclusive = false, multiplicity = "1")
  public ImportSettings importSettings = new ImportSettings();

  @Option(
      names = {"-e", "--dsbulk-use-embedded"},
      description =
          "Use the embedded DSBulk version instead of an external one. "
              + "The default is to use an external DSBulk command.",
      defaultValue = "false")
  public boolean dsbulkEmbedded = false;

  @Option(
      names = {"-c", "--dsbulk-cmd"},
      paramLabel = "CMD",
      description =
          "The external DSBulk command to use. Ignored if the embedded DSBulk is being used. "
              + "The default is simply 'dsbulk', assuming that the command is available through the "
              + "PATH variable contents.",
      defaultValue = "dsbulk")
  public String dsbulkCmd = "dsbulk";

  @Option(
      names = {"-l", "--dsbulk-log-dir"},
      paramLabel = "PATH",
      description =
          "The directory where DSBulk should store its logs. "
              + "The default is a 'logs' subdirectory in the current working directory. "
              + "This subdirectory will be created if it does not exist. "
              + "Each DSBulk operation will create a subdirectory inside the log directory specified here.",
      defaultValue = "logs")
  public Path dsbulkLogDir = Paths.get("logs");

  @Option(
      names = {"-w", "--dsbulk-working-dir"},
      paramLabel = "PATH",
      description =
          "The directory where DSBulk should be executed. "
              + "Ignored if the embedded DSBulk is being used. "
              + "If unspecified, it defaults to the current working directory.")
  public Path dsbulkWorkingDir;

  @Option(
      names = "--max-concurrent-ops",
      paramLabel = "NUM",
      description =
          "The maximum number of concurrent operations (exports and imports) to carry. Default is 1. "
              + "Set this to higher values to allow exports and imports to occur concurrently; "
              + "e.g. with a value of 2, each table will be imported as soon as it is exported, "
              + "while the next table is being exported.",
      defaultValue = "1")
  public int maxConcurrentOps = 1;

  @Option(
      names = "--skip-truncate-confirmation",
      description =
          "Skip truncate confirmation before actually truncating tables. "
              + "Only applicable when migrating counter tables, ignored otherwise.",
      defaultValue = "false")
  public boolean skipTruncateConfirmation = false;

  @Option(
      names = "--truncate-before-export",
      description =
          "Truncate tables before the export instead of after. Default is to truncate after the export. "
              + "Only applicable when migrating counter tables, ignored otherwise.",
      defaultValue = "false")
  public boolean truncateBeforeExport = false;

  @Override
  public Pattern getKeyspacesPattern() {
    return keyspacesPattern;
  }

  @Override
  public Pattern getTablesPattern() {
    return tablesPattern;
  }

  @Override
  public TableType getTableType() {
    return tableType;
  }
}
