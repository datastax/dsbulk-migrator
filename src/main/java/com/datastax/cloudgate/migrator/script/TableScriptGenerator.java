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
package com.datastax.cloudgate.migrator.script;

import com.datastax.cloudgate.migrator.live.ExitStatus;
import com.datastax.cloudgate.migrator.processor.ExportedColumn;
import com.datastax.cloudgate.migrator.processor.TableProcessor;
import com.datastax.cloudgate.migrator.settings.MigrationSettings;
import com.datastax.cloudgate.migrator.utils.TableUtils;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.io.PrintWriter;
import java.util.List;

public class TableScriptGenerator extends TableProcessor {

  private final String tableDataDir;
  private final String exportAckDir;
  private final String importAckDir;
  private final String exportAckFile;
  private final String importAckFile;
  private final String escapedFullyQualifiedTableName;

  public TableScriptGenerator(
      TableMetadata table, MigrationSettings settings, List<ExportedColumn> exportedColumns) {
    super(table, settings, exportedColumns);
    tableDataDir =
        "\"${data_dir}/"
            + this.table.getKeyspace().asInternal()
            + "/"
            + this.table.getName().asInternal()
            + "\"";
    exportAckDir = "\"${data_dir}/__exported__\"";
    importAckDir = "\"${data_dir}/__imported__\"";
    exportAckFile =
        "\"${data_dir}/__exported__/"
            + this.table.getKeyspace().asInternal()
            + "__"
            + this.table.getName().asInternal()
            + ".exported\"";
    importAckFile =
        "\"${data_dir}/__imported__/"
            + this.table.getKeyspace().asInternal()
            + "__"
            + this.table.getName().asInternal()
            + ".imported\"";
    escapedFullyQualifiedTableName =
        this.table.getKeyspace().asCql(true).replace("\"", "\\\"")
            + "."
            + this.table.getName().asCql(true).replace("\"", "\\\"");
  }

  public void printExportScript(PrintWriter writer) {
    writer.println("echo 'Exporting table " + TableUtils.getFullyQualifiedTableName(table) + "'");
    writer.println("if [[ -f " + exportAckFile + " ]]; then");
    writer.println(
        "  echo \"Table "
            + escapedFullyQualifiedTableName
            + ": already exported, skipping (delete this file to re-export: \""
            + exportAckFile
            + "\").\"");
    writer.println("else");
    writer.println("  mkdir -p " + tableDataDir);
    writer.println("  operation_id=" + getOperationIdTemplate(true));
    writer.println("  \"${dsbulk_cmd}\" unload \\");
    writer.println("    $([[ -z \"$host\" ]] || echo \"-h \\\"${host}\\\"\") \\");
    writer.println("    $([[ -z \"$bundle\" ]] || echo \"-b \\\"${bundle}\\\"\") \\");
    writer.println("    $([[ -z \"$username\" ]] || echo \"-u \\\"${username}\\\"\") \\");
    writer.println("    $([[ -z \"$password\" ]] || echo \"-p \\\"${password}\\\"\") \\");
    writer.println("    -url " + tableDataDir + " \\");
    writer.println("    -maxRecords \"$max_records\" \\");
    writer.println("    -maxConcurrentFiles \"$max_concurrent_files\" \\");
    writer.println("    -maxConcurrentQueries \"$max_concurrent_queries\" \\");
    writer.println("    -maxErrors 0 \\");
    writer.println("    --schema.splits \"$splits\" \\");
    writer.println("    -cl \"$consistency_level\" \\");
    writer.println("    -header false \\");
    writer.println("    --monitoring.console false \\");
    writer.println("    --engine.executionId \"$operation_id\" \\");
    writer.println("    -logDir \"${dsbulk_logs}\" \\");
    writer.print("    -query " + buildExportQuery());
    if (!settings.exportSettings.extraDsbulkOptions.isEmpty()) {
      for (String s : settings.exportSettings.extraDsbulkOptions) {
        writer.println(" \\");
        writer.print("    " + s);
      }
    }
    writer.println();
    writer.println("  exit_status=$?");
    writer.println("  if [ $exit_status -eq 0 ]; then");
    writer.println("    echo \"Table " + escapedFullyQualifiedTableName + ": export successful\"");
    writer.println("    mkdir -p " + exportAckDir);
    writer.println("    touch " + exportAckFile);
    writer.println("    echo \"$operation_id\" >> " + exportAckFile);
    writer.println(
        "  elif [ $exit_status -gt " + ExitStatus.STATUS_CRASHED.exitCode() + " ]; then");
    writer.println(
        "    echo \"Table "
            + table.getKeyspace().asCql(true).replace("\"", "\\\"")
            + "."
            + table.getName().asCql(true).replace("\"", "\\\"")
            + ": export failed unexpectedly, aborting migration (exit status: $exit_status).\"");
    writer.println("    exit $exit_status");
    writer.println("  else");
    writer.println(
        "    echo \"Table "
            + escapedFullyQualifiedTableName
            + ": export failed (exit status: $exit_status)\"");
    writer.println("  fi");
    writer.println("fi");
    writer.println();
    writer.flush();
  }

  public void printImportScript(PrintWriter writer) {
    writer.println("echo 'Importing table " + TableUtils.getFullyQualifiedTableName(table) + "'");
    writer.println("if [[ -f " + importAckFile + " ]]; then");
    writer.println(
        "  echo \"Table "
            + escapedFullyQualifiedTableName
            + ": already imported, skipping (delete this file to re-import: \""
            + importAckFile
            + "\").\"");
    writer.println("elif [ ! -d " + tableDataDir + " ]; then");
    writer.println(
        "  echo \"Table "
            + escapedFullyQualifiedTableName
            + ": data directory "
            + tableDataDir
            + " does not exist, skipping. Was the table exported?\"");
    writer.println("elif ls -1qA " + tableDataDir + "/output*.csv 2> /dev/null | grep -q . ; then");
    writer.println("  operation_id=" + getOperationIdTemplate(false));
    writer.println("  \"${dsbulk_cmd}\" load \\");
    writer.println("    $([[ -z \"$host\" ]] || echo \"-h \\\"${host}\\\"\") \\");
    writer.println("    $([[ -z \"$bundle\" ]] || echo \"-b \\\"${bundle}\\\"\") \\");
    writer.println("    $([[ -z \"$username\" ]] || echo \"-u \\\"${username}\\\"\") \\");
    writer.println("    $([[ -z \"$password\" ]] || echo \"-p \\\"${password}\\\"\") \\");
    writer.println("    -url " + tableDataDir + " \\");
    writer.println("    -maxErrors \"$max_errors\" \\");
    writer.println("    -maxConcurrentFiles \"$max_concurrent_files\" \\");
    writer.println("    -maxConcurrentQueries \"$max_concurrent_queries\" \\");
    writer.println("    -cl \"$consistency_level\" \\");
    writer.println("    -header false \\");
    writer.println("    --monitoring.console false \\");
    writer.println("    --engine.executionId \"$operation_id\" \\");
    writer.println("    -logDir \"${dsbulk_logs}\" \\");
    writer.println("    -m " + buildImportMapping() + " \\");
    int regularColumns = countRegularColumns();
    if (regularColumns == 0) {
      writer.println("    -k \"" + escape(table.getKeyspace()) + "\" \\");
      writer.print("    -t \"" + escape(table.getName()) + "\"");
    } else if (regularColumns == 1) {
      writer.print("    -query " + buildSingleImportQuery());
    } else {
      writer.println("    --batch.mode DISABLED \\");
      writer.print("    -query " + buildBatchImportQuery());
    }
    if (!settings.importSettings.extraDsbulkOptions.isEmpty()) {
      for (String s : settings.importSettings.extraDsbulkOptions) {
        writer.println(" \\");
        writer.print("    " + s);
      }
    }
    writer.println();
    writer.println("  exit_status=$?");
    writer.println("  if [ $exit_status -eq 0 ]; then");
    writer.println("    echo \"Table " + escapedFullyQualifiedTableName + ": import successful\"");
    writer.println("    mkdir -p " + importAckDir);
    writer.println("    touch " + importAckFile);
    writer.println("    echo \"$operation_id\" >> " + importAckFile);
    writer.println(
        "  elif [ $exit_status -gt " + ExitStatus.STATUS_CRASHED.exitCode() + " ]; then");
    writer.println(
        "    echo \"Table "
            + table.getKeyspace().asCql(true).replace("\"", "\\\"")
            + "."
            + table.getName().asCql(true).replace("\"", "\\\"")
            + ": import failed unexpectedly, aborting migration (exit status: $exit_status).\"");
    writer.println("    exit $exit_status");
    writer.println("  else");
    writer.println(
        "    echo \"Table "
            + escapedFullyQualifiedTableName
            + ": import failed (exit status: $exit_status)\"");
    writer.println("  fi");
    writer.println("else");
    writer.println(
        "    echo \"Table "
            + escapedFullyQualifiedTableName
            + ": export did not create any CSV file, skipping. Is the table empty?\"");
    writer.println("fi");
    writer.println();
    writer.flush();
  }

  protected String escape(String text) {
    return text.replace("\"", "\\\\\\\"").replace("$", "\\$");
  }

  protected String getImportDefaultTimestamp() {
    return "$default_writetime";
  }

  private String getOperationIdTemplate(boolean export) {
    return (export ? "EXPORT" : "IMPORT")
        + "_"
        + table.getKeyspace().asInternal()
        + "_"
        + table.getName().asInternal()
        + "_"
        + "$(date -u +%Y%m%d_%H%M%S)";
  }
}
