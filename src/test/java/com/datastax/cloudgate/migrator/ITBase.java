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

import static com.datastax.oss.dsbulk.workflow.api.utils.PlatformUtils.isWindows;

import com.datastax.cloudgate.migrator.settings.ExportSettings;
import com.datastax.cloudgate.migrator.settings.ImportSettings;
import com.datastax.cloudgate.migrator.settings.MigrationSettings;
import com.datastax.cloudgate.migrator.utils.LoggingUtils;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Column;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Keyspace;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Table;
import com.datastax.oss.dsbulk.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SimulacronConfig(dseVersion = "")
abstract class ITBase {

  final BoundCluster origin;
  final BoundCluster target;

  final String originHost;
  final String targetHost;

  Path dataDir;
  Path logsDir;
  Path tableDir;
  Path dsbulkDir;
  Path exportAck;
  Path importAck;
  Path exportScript;
  Path importScript;

  ITBase(BoundCluster origin, BoundCluster target) {
    this.origin = origin;
    this.target = target;
    originHost =
        this.origin.node(0).inetSocketAddress().getHostString()
            + ':'
            + this.origin.node(0).inetSocketAddress().getPort();
    targetHost =
        this.target.node(0).inetSocketAddress().getHostString()
            + ':'
            + this.target.node(0).inetSocketAddress().getPort();
  }

  @BeforeAll
  void extractExternalDsbulk() throws IOException {
    dsbulkDir = Files.createTempDirectory("dsbulk");
    try (TarArchiveInputStream tar =
        new TarArchiveInputStream(
            new GzipCompressorInputStream(
                new BufferedInputStream(getClass().getResourceAsStream("/dsbulk-1.7.0.tar.gz"))))) {
      ArchiveEntry entry;
      while ((entry = tar.getNextEntry()) != null) {
        Path src = Paths.get(entry.getName());
        Path dest = dsbulkDir.resolve(src);
        if (entry.isDirectory()) {
          if (!Files.exists(dest)) {
            Files.createDirectory(dest);
          }
        } else {
          Files.copy(tar, dest);
          if (dest.endsWith("dsbulk") || dest.endsWith("dsbulk.cmd")) {
            Files.setPosixFilePermissions(
                dest,
                Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.GROUP_EXECUTE));
          }
        }
      }
    }
  }

  @BeforeEach
  void resetLogging() throws Exception {
    LoggingUtils.configureLogging(getClass().getResource("/logback-test.xml"));
  }

  @BeforeEach
  void createTempDirs() throws IOException {
    dataDir = Files.createTempDirectory("data");
    logsDir = Files.createTempDirectory("logs");
    tableDir = dataDir.resolve("test").resolve("t1");
    exportAck = dataDir.resolve("__exported__").resolve("test__t1.exported");
    importAck = dataDir.resolve("__imported__").resolve("test__t1.imported");
    exportScript = dataDir.resolve("cloud-gate-migrator-export.sh");
    importScript = dataDir.resolve("cloud-gate-migrator-import.sh");
  }

  @AfterEach
  void deleteTempDirs() {
    if (dataDir != null && Files.exists(dataDir)) {
      FileUtils.deleteDirectory(dataDir);
    }
    if (logsDir != null && Files.exists(logsDir)) {
      FileUtils.deleteDirectory(logsDir);
    }
  }

  @AfterAll
  void deleteDsbulkDir() {
    if (dsbulkDir != null && Files.exists(dsbulkDir)) {
      FileUtils.deleteDirectory(dsbulkDir);
    }
  }

  @BeforeEach
  void resetSimulacron() {

    origin.clearLogs();
    origin.clearPrimes(true);

    target.clearLogs();
    target.clearPrimes(true);

    SimulacronUtils.primeTables(
        origin,
        new Keyspace(
            "test",
            new Table(
                "t1",
                new Column("pk", DataTypes.INT),
                new Column("cc", DataTypes.INT),
                new Column("v", DataTypes.INT))));

    Map<String, String> columnTypes =
        map("pk", "int", "cc", "int", "v", "int", "v_writetime", "bigint", "v_ttl", "int");

    origin.prime(
        PrimeDsl.when(
                "SELECT pk, cc, v, WRITETIME(v) AS v_writetime, TTL(v) AS v_ttl FROM test.t1 WHERE token(pk) > :start AND token(pk) <= :end")
            .then(
                new SuccessResult(
                    List.of(
                        map("pk", 1, "cc", 1, "v", 1, "v_writetime", 12456L, "v_ttl", null),
                        map("pk", 1, "cc", 2, "v", 2, "v_writetime", 234567L, "v_ttl", 100)),
                    columnTypes)));

    origin.prime(
        PrimeDsl.when(
                new Query(
                    "INSERT INTO test.t1 (pk, cc, v) VALUES (:pk, :cc, :v) USING TIMESTAMP :v_writetime AND TTL :v_ttl",
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    columnTypes))
            .then(new SuccessResult(Collections.emptyList(), Collections.emptyMap())));
  }

  MigrationSettings createSettings(boolean embedded) {
    MigrationSettings settings = new MigrationSettings();
    settings.generalSettings.dataDir = dataDir;
    settings.exportSettings.clusterInfo = new ExportSettings.ClusterInfo();
    settings.importSettings.clusterInfo = new ImportSettings.ClusterInfo();
    settings.exportSettings.clusterInfo.hostAndPort = HostAndPort.fromString(originHost);
    settings.importSettings.clusterInfo.hostAndPort = HostAndPort.fromString(targetHost);
    settings.dsBulkSettings.dsbulkEmbedded = embedded;
    settings.dsBulkSettings.dsbulkLogDir = logsDir;
    if (!embedded) {
      if (isWindows()) {
        settings.dsBulkSettings.dsbulkCmd =
            dsbulkDir.resolve("bin").resolve("dsbulk.cmd").toString();
      } else {
        settings.dsBulkSettings.dsbulkCmd = dsbulkDir.resolve("bin").resolve("dsbulk").toString();
      }
    }
    return settings;
  }

  @SuppressWarnings("deprecation")
  List<QueryLog> countInsertions() {
    return target.getLogs().getQueryLogs().stream()
        .filter(
            l ->
                l.getType().equals("EXECUTE")
                    && l.getQuery() != null
                    && l.getQuery().startsWith("INSERT INTO test.t1"))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  static <K, V> Map<K, V> map(Object... keysAndValues) {
    Map<Object, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length - 1; i += 2) {
      map.put(keysAndValues[i], keysAndValues[i + 1]);
    }
    return (Map<K, V>) map;
  }
}
