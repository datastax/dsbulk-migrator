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
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
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
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableProcessorFactory<T extends TableProcessor> {

  public static final Logger LOGGER = LoggerFactory.getLogger(TableProcessorFactory.class);

  public List<T> create(SchemaSettings settings) {
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, "DcInferringLoadBalancingPolicy")
            .build();
    CqlSessionBuilder builder =
        CqlSession.builder()
            .addContactPoint(settings.getExportHostAddress())
            .withConfigLoader(loader);
    if (settings.getExportUsername().isPresent() && settings.getExportPassword().isPresent()) {
      builder.withAuthCredentials(
          settings.getExportUsername().get(), settings.getExportPassword().get());
    }
    builder.withNodeFilter(
        node -> node.getEndPoint().resolve().equals(settings.getExportHostAddress()));
    List<T> processors = new ArrayList<>();
    try (CqlSession session = builder.build()) {
      List<CqlIdentifier> exportKeyspaces = settings.getExportKeyspaces();
      if (exportKeyspaces.isEmpty()) {
        exportKeyspaces = allKeyspaces(session);
      }
      LOGGER.info("Keyspaces to migrate: {}", exportKeyspaces);
      for (CqlIdentifier keyspaceName : exportKeyspaces) {
        KeyspaceMetadata keyspace = session.getMetadata().getKeyspaces().get(keyspaceName);
        if (keyspace == null) {
          throw new IllegalArgumentException("Keyspace does not exist: " + keyspaceName);
        }
        for (TableMetadata table : keyspace.getTables().values()) {
          List<ExportedColumn> exportedColumns = buildExportedColumns(table, session);
          processors.add(create(table, settings, exportedColumns));
        }
      }
    }
    return processors;
  }

  protected abstract T create(
      TableMetadata table, SchemaSettings settings, List<ExportedColumn> exportedColumns);

  private static List<CqlIdentifier> allKeyspaces(CqlSession session) {
    return session.getMetadata().getKeyspaces().keySet().stream()
        .filter(
            ks ->
                !ks.asInternal().startsWith("system_")
                    && !ks.asInternal().startsWith("dse_")
                    && !ks.asInternal().equals("system")
                    && !ks.asInternal().equals("OpsCenter"))
        .collect(Collectors.toList());
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
