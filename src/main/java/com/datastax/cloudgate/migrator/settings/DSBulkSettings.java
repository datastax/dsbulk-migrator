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
import picocli.CommandLine.Option;

public class DSBulkSettings {

  @Option(
      names = "--dsbulk-use-embedded",
      description =
          "Use the embedded DSBulk version instead of an external one. "
              + "The default is to use an external DSBulk command.",
      defaultValue = "false")
  public boolean dsbulkEmbedded = false;

  @Option(
      names = "--dsbulk-external-cmd",
      paramLabel = "CMD",
      description =
          "The external DSBulk command to use. Ignored if the embedded DSBulk is being used. "
              + "The default is simply 'dsbulk', assuming that the command is available through the "
              + "PATH variable contents.",
      defaultValue = "dsbulk")
  public String dsbulkCmd = "dsbulk";

  @Option(
      names = "--dsbulk-log-dir",
      paramLabel = "PATH",
      description =
          "The directory where DSBulk should store its logs. "
              + "The default is a 'logs' subdirectory in the current working directory. "
              + "This subdirectory will be created if it does not exist. "
              + "Each DSBulk operation will create a subdirectory inside the log directory specified here.",
      defaultValue = "logs")
  public Path dsbulkLogDir = Paths.get("logs");

  @Option(
      names = "--dsbulk-working-dir",
      paramLabel = "PATH",
      description =
          "The directory where DSBulk should be executed. "
              + "Ignored if the embedded DSBulk is being used. "
              + "If unspecified, it defaults to the current working directory.")
  public Path dsbulkWorkingDir;
}
