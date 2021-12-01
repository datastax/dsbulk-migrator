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
package com.datastax.cloudgate.migrator.utils;

import com.datastax.cloudgate.migrator.model.ExportedColumn;
import com.datastax.cloudgate.migrator.model.ExportedTable;
import com.datastax.cloudgate.migrator.model.TableType;
import com.datastax.cloudgate.migrator.settings.ClusterInfo;
import com.datastax.cloudgate.migrator.settings.Credentials;
import com.datastax.cloudgate.migrator.settings.InclusionSettings;
import com.datastax.cloudgate.migrator.settings.TlsSettings;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelUtils {

  public static final Logger LOGGER = LoggerFactory.getLogger(ModelUtils.class);

  public static List<ExportedTable> buildExportedTables(
      ClusterInfo origin,
      Credentials credentials,
      TlsSettings tlsSettings,
      InclusionSettings inclusionSettings) {
    List<ExportedTable> exportedTables = new ArrayList<>();
    try (CqlSession session = SessionUtils.createSession(origin, credentials, tlsSettings)) {
      LOGGER.info("Tables to migrate:");
      List<KeyspaceMetadata> keyspaces = getExportedKeyspaces(session, inclusionSettings);
      for (KeyspaceMetadata keyspace : keyspaces) {
        List<TableMetadata> tables = getExportedTablesInKeyspace(keyspace, inclusionSettings);
        for (TableMetadata table : tables) {
          List<ExportedColumn> exportedColumns = buildExportedColumns(table, session);
          ExportedTable exportedTable = new ExportedTable(keyspace, table, exportedColumns);
          exportedTables.add(exportedTable);
          LOGGER.info(
              "- {} ({})", exportedTable, exportedTable.counterTable ? "counter" : "regular");
        }
      }
    }
    return exportedTables;
  }

  private static List<KeyspaceMetadata> getExportedKeyspaces(
      CqlSession session, InclusionSettings settings) {
    Pattern exportKeyspaces = settings.getKeyspacesPattern();
    return session.getMetadata().getKeyspaces().values().stream()
        .filter(ks -> exportKeyspaces.matcher(ks.getName().asInternal()).matches())
        .collect(Collectors.toList());
  }

  private static List<TableMetadata> getExportedTablesInKeyspace(
      KeyspaceMetadata keyspace, InclusionSettings settings) {
    Pattern exportTables = settings.getTablesPattern();
    TableType tableType = settings.getTableType();
    return keyspace.getTables().values().stream()
        .sorted(Comparator.comparing(t -> t.getName().asInternal()))
        .filter(table -> !table.isVirtual())
        .filter(table -> exportTables.matcher(table.getName().asInternal()).matches())
        .filter(table -> tableType == TableType.all || tableType == getTableType(table))
        .collect(Collectors.toList());
  }

  private static TableType getTableType(TableMetadata table) {
    return TableUtils.isCounterTable(table) ? TableType.counter : TableType.regular;
  }

  private static List<ExportedColumn> buildExportedColumns(
      TableMetadata table, CqlSession session) {
    List<ExportedColumn> exportedColumns = new ArrayList<>();
    for (ColumnMetadata pk : table.getPrimaryKey()) {
      exportedColumns.add(new ExportedColumn(pk, true, null, null));
    }
    for (ColumnMetadata col : table.getColumns().values()) {
      if (table.getPrimaryKey().contains(col)) {
        continue;
      }
      CqlIdentifier writetime = null;
      if (isFunctionAvailable(table, col, session, "WRITETIME")) {
        writetime = writetimeId(col);
      }
      CqlIdentifier ttl = null;
      if (isFunctionAvailable(table, col, session, "TTL")) {
        ttl = ttlId(col);
      }
      exportedColumns.add(new ExportedColumn(col, false, writetime, ttl));
    }
    return exportedColumns;
  }

  private static boolean isFunctionAvailable(
      TableMetadata table, ColumnMetadata col, CqlSession session, String function) {
    if (table.getPrimaryKey().contains(col)) {
      return false;
    }
    if (col.getType().getProtocolCode() == ProtocolConstants.DataType.COUNTER) {
      return false;
    }
    if (col.getType() instanceof MapType
        || col.getType() instanceof ListType
        || col.getType() instanceof SetType
        || col.getType() instanceof UserDefinedType) {
      try {
        ResultSet rs =
            session.execute(
                "SELECT "
                    + function
                    + "("
                    + col.getName().asCql(true)
                    + ") FROM "
                    + table.getKeyspace().asCql(true)
                    + "."
                    + table.getName().asCql(true));
        if (rs.getColumnDefinitions().get(0).getType() instanceof ListType) {
          // multiple elements returned: cannot import the result
          return false;
        }
      } catch (Exception e) {
        return false;
      }
    }
    return true;
  }

  private static CqlIdentifier writetimeId(ColumnMetadata col) {
    return CqlIdentifier.fromInternal(col.getName().asInternal() + "_writetime");
  }

  private static CqlIdentifier ttlId(ColumnMetadata col) {
    return CqlIdentifier.fromInternal(col.getName().asInternal() + "_ttl");
  }
}
