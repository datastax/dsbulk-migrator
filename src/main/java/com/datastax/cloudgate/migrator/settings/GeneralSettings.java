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
package com.datastax.cloudgate.migrator.settings;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import picocli.CommandLine.Option;

public class GeneralSettings {

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
  public Pattern keyspaces = Pattern.compile("^(?!system|dse|OpsCenter)\\w+$");

  @Option(
      names = {"-t", "--tables"},
      paramLabel = "REGEX",
      description =
          "A regular expression to select tables to migrate."
              + "The default is to migrate all tables inside the keyspaces that were selected for migration with --keyspaces. "
              + "Case-sensitive table names must be entered in their exact case.",
      defaultValue = ".*")
  public Pattern tables = Pattern.compile(".*");

  @Option(
      names = "--table-types",
      paramLabel = "regular|counter|all",
      description = "The table types to migrate (regular, counter, or all). Default is all.",
      defaultValue = "all")
  public TableType tableType = TableType.all;

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
}
