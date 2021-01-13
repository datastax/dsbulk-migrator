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

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class MigrationSettings {

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message.")
  boolean usageHelpRequested;

  @ArgGroup(exclusive = false, multiplicity = "1")
  public GeneralSettings generalSettings = new GeneralSettings();

  @ArgGroup(exclusive = false, multiplicity = "1")
  public ExportSettings exportSettings = new ExportSettings();

  @ArgGroup(exclusive = false, multiplicity = "1")
  public ImportSettings importSettings = new ImportSettings();

  @ArgGroup(exclusive = false, multiplicity = "1")
  public GlobalDSBulkSettings dsBulkSettings = new GlobalDSBulkSettings();
}
