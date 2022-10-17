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
package com.datastax.migrator;

import com.datastax.migrator.ddl.DdlGenerationSettings;
import com.datastax.migrator.ddl.SchemaDdlGenerator;
import com.datastax.migrator.live.LiveMigrationSettings;
import com.datastax.migrator.live.SchemaLiveMigrator;
import com.datastax.migrator.script.SchemaScriptGenerator;
import com.datastax.migrator.script.ScriptGenerationSettings;
import com.datastax.migrator.settings.VersionProvider;
import com.datastax.migrator.utils.LoggingUtils;
import java.io.IOException;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "DSBulkMigrator",
    description =
        "A tool to migrate historical data between two clusters, "
            + "leveraging the DataStax Bulk Loader (DSBulk) to perform the actual data migration.",
    versionProvider = VersionProvider.class,
    sortOptions = false,
    usageHelpWidth = 100)
public class DSBulkMigrator {

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message.")
  boolean usageHelpRequested;

  @Option(
      names = {"-v", "--version"},
      versionHelp = true,
      description = "Displays version info.")
  boolean versionInfoRequested;

  public static void main(String[] args) throws Exception {
    LoggingUtils.configureLogging(LoggingUtils.MIGRATOR_CONFIGURATION_FILE);
    CommandLine commandLine = new CommandLine(new DSBulkMigrator());
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Command(
      name = "migrate-live",
      description =
          "Starts a live data migration using a pre-existing DSBulk installation, "
              + "or alternatively, the embedded DSBulk version.",
      optionListHeading = "Available options:%n",
      abbreviateSynopsis = true,
      usageHelpWidth = 100)
  private int migrateLive(
      @ArgGroup(exclusive = false, multiplicity = "1") LiveMigrationSettings settings) {
    SchemaLiveMigrator schemaLiveMigrator = new SchemaLiveMigrator(settings);
    return schemaLiveMigrator.migrate().exitCode();
  }

  @Command(
      name = "generate-script",
      description =
          "Generates a script that, once executed, will perform a live data migration, "
              + "using a pre-existing DSBulk installation.",
      optionListHeading = "Available options:%n",
      abbreviateSynopsis = true,
      usageHelpWidth = 100)
  private int generateScript(
      @ArgGroup(exclusive = false, multiplicity = "1") ScriptGenerationSettings settings)
      throws IOException {
    SchemaScriptGenerator schemaScriptGenerator = new SchemaScriptGenerator(settings);
    return schemaScriptGenerator.generate().exitCode();
  }

  @Command(
      name = "generate-ddl",
      description =
          "Reads the schema from the origin cluster and "
              + "generates CQL files to recreate it in the target cluster.",
      optionListHeading = "Available options:%n",
      abbreviateSynopsis = true,
      usageHelpWidth = 100)
  private int generateDdl(
      @ArgGroup(exclusive = false, multiplicity = "1") DdlGenerationSettings settings)
      throws IOException {
    SchemaDdlGenerator schemaDdlGenerator = new SchemaDdlGenerator(settings);
    return schemaDdlGenerator.generate().exitCode();
  }
}
