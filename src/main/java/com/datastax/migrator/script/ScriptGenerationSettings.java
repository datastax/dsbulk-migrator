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
package com.datastax.migrator.script;

import com.datastax.migrator.model.TableType;
import com.datastax.migrator.settings.ExportSettings;
import com.datastax.migrator.settings.ImportSettings;
import com.datastax.migrator.settings.InclusionSettings;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ScriptGenerationSettings implements InclusionSettings {

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message.")
  boolean usageHelpRequested;

  @Option(
      names = {"-d", "--data-dir"},
      paramLabel = "PATH",
      description =
          "The directory where migration scripts will be generated. "
              + "The default is a 'data' subdirectory in the current working directory. "
              + "The data directory will be created if it does not exist. ",
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
      names = {"-c", "--dsbulk-cmd"},
      paramLabel = "CMD",
      description =
          "The DSBulk command to use. "
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
