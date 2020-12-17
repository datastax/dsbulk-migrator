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
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.Iterator;
import java.util.List;

public abstract class TableProcessor {

  protected final TableMetadata table;
  protected final MigrationSettings settings;
  protected final List<ExportedColumn> exportedColumns;

  public TableProcessor(
      TableMetadata table, MigrationSettings settings, List<ExportedColumn> exportedColumns) {
    this.table = table;
    this.settings = settings;
    this.exportedColumns = exportedColumns;
  }

  public String getFullyQualifiedTableName() {
    return String.format("%s.%s", table.getKeyspace().asCql(true), table.getName().asCql(true));
  }

  protected String buildExportQuery() {
    StringBuilder builder = new StringBuilder("\"SELECT ");
    Iterator<ExportedColumn> cols = exportedColumns.iterator();
    while (cols.hasNext()) {
      ExportedColumn exportedColumn = cols.next();
      String name = escape(exportedColumn.col.getName());
      builder.append(name);
      if (exportedColumn.writetime != null) {
        builder.append(", WRITETIME(");
        builder.append(name);
        builder.append(") AS ");
        builder.append(escape(exportedColumn.writetime));
      }
      if (exportedColumn.ttl != null) {
        builder.append(", TTL(");
        builder.append(name);
        builder.append(") AS ");
        builder.append(escape(exportedColumn.ttl));
      }
      if (cols.hasNext()) {
        builder.append(", ");
      }
    }
    builder.append(" FROM ");
    builder.append(escape(table.getKeyspace()));
    builder.append(".");
    builder.append(escape(table.getName()));
    builder.append("\"");
    return builder.toString();
  }

  protected String buildSingleImportQuery() {
    StringBuilder builder = new StringBuilder("\"INSERT INTO ");
    builder.append(escape(table.getKeyspace()));
    builder.append(".");
    builder.append(escape(table.getName()));
    builder.append(" (");
    CqlIdentifier singleWritetime = null;
    CqlIdentifier singleTtl = null;
    Iterator<ExportedColumn> cols = exportedColumns.iterator();
    while (cols.hasNext()) {
      ExportedColumn exportedColumn = cols.next();
      if (exportedColumn.writetime != null) {
        assert singleWritetime == null;
        singleWritetime = exportedColumn.writetime;
      }
      if (exportedColumn.ttl != null) {
        assert singleTtl == null;
        singleTtl = exportedColumn.ttl;
      }
      String name = escape(exportedColumn.col.getName());
      builder.append(name);
      if (cols.hasNext()) {
        builder.append(", ");
      }
    }
    builder.append(") VALUES (");
    cols = exportedColumns.iterator();
    while (cols.hasNext()) {
      ExportedColumn exportedColumn = cols.next();
      if (exportedColumn.writetime != null) {
        singleWritetime = exportedColumn.writetime;
      }
      if (exportedColumn.ttl != null) {
        singleTtl = exportedColumn.ttl;
      }
      String name = escape(exportedColumn.col.getName());
      builder.append(":");
      builder.append(name);
      if (cols.hasNext()) {
        builder.append(", ");
      }
    }
    builder.append(") USING TIMESTAMP ");
    if (singleWritetime != null) {
      builder.append(":");
      builder.append(escape(singleWritetime));
    } else {
      builder.append(getImportDefaultTimestamp());
    }
    if (singleTtl != null) {
      builder.append(" AND TTL :");
      builder.append(escape(singleTtl));
    }
    builder.append("\"");
    return builder.toString();
  }

  protected String buildBatchImportQuery() {
    StringBuilder builder = new StringBuilder("\"BEGIN BATCH ");
    Iterator<ExportedColumn> cols = exportedColumns.stream().filter(col -> !col.pk).iterator();
    while (cols.hasNext()) {
      ExportedColumn exportedColumn = cols.next();
      builder.append("INSERT INTO ");
      builder.append(escape(table.getKeyspace()));
      builder.append(".");
      builder.append(escape(table.getName()));
      builder.append(" (");
      for (ColumnMetadata pk : table.getPrimaryKey()) {
        builder.append(escape(pk.getName()));
        builder.append(", ");
      }
      builder.append(escape(exportedColumn.col.getName()));
      builder.append(") VALUES (");
      for (ColumnMetadata pk : table.getPrimaryKey()) {
        builder.append(":");
        builder.append(escape(pk.getName()));
        builder.append(", ");
      }
      builder.append(":");
      builder.append(escape(exportedColumn.col.getName()));
      builder.append(") USING TIMESTAMP ");
      if (exportedColumn.writetime != null) {
        builder.append(":");
        builder.append(escape(exportedColumn.writetime));
      } else {
        builder.append(getImportDefaultTimestamp());
      }
      if (exportedColumn.ttl != null) {
        builder.append(" AND TTL :");
        builder.append(escape(exportedColumn.ttl));
      }
      builder.append("; ");
    }
    builder.append("APPLY BATCH\"");
    return builder.toString();
  }

  protected String buildImportMapping() {
    StringBuilder builder = new StringBuilder("\"");
    Iterator<ExportedColumn> cols = exportedColumns.iterator();
    while (cols.hasNext()) {
      ExportedColumn exportedColumn = cols.next();
      builder.append(escape(exportedColumn.col.getName()));
      if (exportedColumn.writetime != null) {
        builder.append(",");
        builder.append(escape(exportedColumn.writetime));
      }
      if (exportedColumn.ttl != null) {
        builder.append(",");
        builder.append(escape(exportedColumn.ttl));
      }
      if (cols.hasNext()) {
        builder.append(",");
      }
    }
    builder.append("\"");
    return builder.toString();
  }

  protected int countRegularColumns() {
    int regularCols = 0;
    for (ExportedColumn exportedColumn : exportedColumns) {
      if (!exportedColumn.pk
          && !(exportedColumn.col.getType().getProtocolCode()
              == ProtocolConstants.DataType.COUNTER)) {
        regularCols++;
      }
    }
    return regularCols;
  }

  protected String escape(CqlIdentifier id) {
    return escape(id.asCql(true));
  }

  protected abstract String escape(String id);

  protected abstract String getImportDefaultTimestamp();
}
