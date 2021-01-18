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
package com.datastax.cloudgate.migrator.ddl;

import com.datastax.cloudgate.migrator.settings.ExportSettings.ExportClusterInfo;
import com.datastax.cloudgate.migrator.settings.ExportSettings.ExportCredentials;
import com.datastax.cloudgate.migrator.settings.InclusionSettings;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class DdlGenerationSettings implements InclusionSettings {

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message.")
  boolean usageHelpRequested;

  @Option(
      names = {"-d", "--data-dir"},
      paramLabel = "PATH",
      description =
          "The directory where CQL files will be generated. "
              + "The default is a 'data' subdirectory in the current working directory. "
              + "The data directory will be created if it does not exist. ",
      defaultValue = "data")
  public Path dataDir = Paths.get("data");

  @Option(
      names = {"-k", "--keyspaces"},
      paramLabel = "REGEX",
      description =
          "A regular expression to select keyspaces to export. "
              + "The default is to export all keyspaces except system keyspaces, DSE-specific keyspaces, and the OpsCenter keyspace. "
              + "Case-sensitive keyspace names must be entered in their exact case.",
      defaultValue = "^(?!system|dse|OpsCenter)\\w+$")
  public Pattern keyspacesPattern = Pattern.compile("^(?!system|dse|OpsCenter)\\w+$");

  @Option(
      names = {"-t", "--tables"},
      paramLabel = "REGEX",
      description =
          "A regular expression to select tables to export."
              + "The default is to export all tables inside the keyspaces that were selected for export with --keyspaces. "
              + "Case-sensitive table names must be entered in their exact case.",
      defaultValue = ".*")
  public Pattern tablesPattern = Pattern.compile(".*");

  @Option(
      names = {"-a", "--optimize-for-astra"},
      description =
          "Produce CQL scripts optimized for DataStax Astra. "
              + "Astra does not allow some options in DDL statements; "
              + "using this option, forbidden options will be omitted from the generated CQL files.",
      defaultValue = "false")
  public boolean optimizeForAstra = false;

  @ArgGroup(multiplicity = "1")
  public ExportClusterInfo clusterInfo;

  @ArgGroup(exclusive = false)
  public ExportCredentials credentials;

  @Override
  public Pattern getKeyspacesPattern() {
    return keyspacesPattern;
  }

  @Override
  public Pattern getTablesPattern() {
    return tablesPattern;
  }
}
