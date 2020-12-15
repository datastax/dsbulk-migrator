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
package com.datastax.cloudgate.migrator;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.io.PrintWriter;
import java.util.List;

public class TableGenerator extends TableProcessor {

  public TableGenerator(
      TableMetadata table, SchemaSettings settings, List<ExportedColumn> exportedColumns) {
    super(table, settings, exportedColumns);
  }

  public void printExportScript(PrintWriter writer) {
    writer.print("echo 'Exporting table ");
    writer.print(table.getKeyspace().asCql(true));
    writer.print(".");
    writer.print(table.getName().asCql(true));
    writer.println("'");
    String exportDir =
        "\"${export_dir}/"
            + table.getKeyspace().asInternal()
            + "/"
            + table.getName().asInternal()
            + "\"";
    writer.println("mkdir -p " + exportDir);
    writer.println("\"${dsbulk_cmd}\" unload \\");
    writer.println("  $([[ -z \"$host\" ]] || echo \"-h \\\"${host}\\\"\") \\");
    writer.println("  $([[ -z \"$bundle\" ]] || echo \"-b \\\"${bundle}\\\"\") \\");
    writer.println("  $([[ -z \"$username\" ]] || echo \"-u \\\"${username}\\\"\") \\");
    writer.println("  $([[ -z \"$password\" ]] || echo \"-p \\\"${password}\\\"\") \\");
    writer.println("  -url " + exportDir + " \\");
    writer.println("  -maxRecords $max_records \\");
    writer.println("  -maxConcurrentFiles $max_concurrent_files \\");
    writer.println("  -maxConcurrentQueries $max_concurrent_queries \\");
    writer.println("  --schema.splits $splits \\");
    writer.println("  -cl $consistency_level \\");
    writer.println("  -header false \\");
    writer.println("  --monitoring.console false \\");
    writer.println("  --engine.executionId \"" + buildOperationId("EXPORT") + "\" \\");
    writer.println("  -logDir \"${dsbulk_logs}\" \\");
    writer.println("  -query " + buildExportQuery());
    writer.println(
        "if [ $? -eq 0 ]; then echo \"Export successful\"; else echo \"Export failed\"; exit 1; fi");
    writer.println();
  }

  public void printImportScript(PrintWriter writer) {
    writer.print("echo 'Importing table ");
    writer.print(table.getKeyspace().asCql(true));
    writer.print(".");
    writer.print(table.getName().asCql(true));
    writer.println("'");
    writer.println("\"${dsbulk_cmd}\" load \\");
    writer.println("  $([[ -z \"$host\" ]] || echo \"-h \\\"${host}\\\"\") \\");
    writer.println("  $([[ -z \"$bundle\" ]] || echo \"-b \\\"${bundle}\\\"\") \\");
    writer.println("  $([[ -z \"$username\" ]] || echo \"-u \\\"${username}\\\"\") \\");
    writer.println("  $([[ -z \"$password\" ]] || echo \"-p \\\"${password}\\\"\") \\");
    writer.println(
        "  -url \"${import_dir}/"
            + table.getKeyspace().asInternal()
            + "/"
            + table.getName().asInternal()
            + "\" \\");
    writer.println("  -maxConcurrentFiles $max_concurrent_files \\");
    writer.println("  -maxConcurrentQueries $max_concurrent_queries \\");
    writer.println("  -cl $consistency_level \\");
    writer.println("  -header false \\");
    writer.println("  --monitoring.console false \\");
    writer.println("  --engine.executionId \"" + buildOperationId("IMPORT") + "\" \\");
    writer.println("  -logDir \"${dsbulk_logs}\" \\");
    writer.println("  -m " + buildImportMapping() + " \\");
    int regularColumns = countRegularColumns();
    if (regularColumns == 0) {
      writer.println("  -k \"" + escape(table.getKeyspace()) + "\" \\");
      writer.println("  -t \"" + escape(table.getName()) + "\" \\");
    } else if (regularColumns == 1) {
      writer.println("  -query " + buildSingleImportQuery());
    } else {
      writer.println("  --batch.mode DISABLED \\");
      writer.println("  -query " + buildBatchImportQuery());
    }
    writer.println(
        "if [ $? -eq 0 ]; then echo \"Import successful\"; else echo \"Import failed\"; exit 1; fi");
    writer.println();
  }

  protected String escape(CqlIdentifier id) {
    return id.asCql(true).replace("\"", "\\\\\\\"");
  }

  protected String getImportDefaultTimestamp() {
    return "$default_writetime";
  }

  private String buildOperationId(String prefix) {
    return prefix
        + "_"
        + table.getKeyspace().asInternal()
        + "_"
        + table.getName().asInternal()
        + "_"
        + "%2\\$tY"
        + "%2\\$tm"
        + "%2\\$td"
        + "_"
        + "%2\\$tH"
        + "%2\\$tM"
        + "%2\\$tS"
        + "_"
        + "%2\\$tL";
  }
}
