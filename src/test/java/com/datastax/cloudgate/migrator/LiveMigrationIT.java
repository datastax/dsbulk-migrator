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

import static com.datastax.oss.dsbulk.tests.utils.FileUtils.readAllLinesInDirectoryAsStream;
import static com.datastax.oss.dsbulk.workflow.api.utils.PlatformUtils.isWindows;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.cloudgate.migrator.live.LiveMigrationSettings;
import com.datastax.cloudgate.migrator.live.SchemaLiveMigrator;
import com.datastax.cloudgate.migrator.settings.ExportSettings.ExportClusterInfo;
import com.datastax.cloudgate.migrator.settings.ImportSettings.ImportClusterInfo;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LiveMigrationIT extends SimulacronITBase {

  Path dataDir;
  Path logsDir;
  Path tableDir;
  Path exportAck;
  Path importAck;
  Path dsbulkDir;

  LiveMigrationIT(BoundCluster origin, BoundCluster target) {
    super(origin, target);
  }

  @Test
  void should_migrate_with_embedded_dsbulk() throws IOException {
    // given
    LiveMigrationSettings settings = createSettings(true);
    // when
    SchemaLiveMigrator migrator = new SchemaLiveMigrator(settings);
    migrator.migrate();
    // then
    assertThat(tableDir).exists().isDirectory();
    assertThat(exportAck).exists().isRegularFile();
    assertThat(importAck).exists().isRegularFile();
    assertThat(readAllLinesInDirectoryAsStream(tableDir).collect(toList()))
        .containsExactlyInAnyOrder(
            "1,1,1,1970-01-01T00:00:00.012456Z,", "1,2,2,1970-01-01T00:00:00.234567Z,100");
    List<QueryLog> insertions = countInsertions();
    assertThat(insertions.size()).isEqualTo(2);
  }

  @Test
  void should_migrate_with_external_dsbulk() throws IOException {
    // given
    extractExternalDsbulk();
    LiveMigrationSettings settings = createSettings(false);
    // when
    SchemaLiveMigrator migrator = new SchemaLiveMigrator(settings);
    migrator.migrate();
    // then
    assertThat(tableDir).exists().isDirectory();
    assertThat(exportAck).exists().isRegularFile();
    assertThat(importAck).exists().isRegularFile();
    assertThat(readAllLinesInDirectoryAsStream(tableDir).collect(toList()))
        .containsExactlyInAnyOrder(
            "1,1,1,1970-01-01T00:00:00.012456Z,", "1,2,2,1970-01-01T00:00:00.234567Z,100");
    List<QueryLog> insertions = countInsertions();
    assertThat(insertions.size()).isEqualTo(2);
  }

  @BeforeEach
  void createTempDirs() throws IOException {
    dataDir = Files.createTempDirectory("data");
    logsDir = Files.createTempDirectory("logs");
    tableDir = dataDir.resolve("test").resolve("t1");
    exportAck = dataDir.resolve("__exported__").resolve("test__t1.exported");
    importAck = dataDir.resolve("__imported__").resolve("test__t1.imported");
  }

  @AfterEach
  void deleteTempDirs() {
    if (dataDir != null && Files.exists(dataDir)) {
      FileUtils.deleteDirectory(dataDir);
    }
    if (logsDir != null && Files.exists(logsDir)) {
      FileUtils.deleteDirectory(logsDir);
    }
    if (dsbulkDir != null && Files.exists(dsbulkDir)) {
      FileUtils.deleteDirectory(dsbulkDir);
    }
  }

  @BeforeAll
  void setLogConfigurationFile() throws URISyntaxException {
    // required because the embedded mode resets the logging configuration after
    // each DSBulk invocation.
    System.setProperty(
        "logback. configurationFile",
        Paths.get(getClass().getResource("/logback-test.xml").toURI()).toString());
  }

  private LiveMigrationSettings createSettings(boolean embedded) {
    LiveMigrationSettings settings = new LiveMigrationSettings();
    settings.dataDir = dataDir;
    settings.exportSettings.clusterInfo = new ExportClusterInfo();
    settings.importSettings.clusterInfo = new ImportClusterInfo();
    settings.exportSettings.clusterInfo.hostsAndPorts =
        Collections.singletonList(HostAndPort.fromString(originHost));
    settings.importSettings.clusterInfo.hostsAndPorts =
        Collections.singletonList(HostAndPort.fromString(targetHost));
    settings.dsbulkEmbedded = embedded;
    settings.dsbulkLogDir = logsDir;
    settings.importSettings.extraDsbulkOptions =
        List.of("--datastax-java-driver.advanced.protocol.version=V3");
    settings.exportSettings.extraDsbulkOptions =
        List.of("--datastax-java-driver.advanced.protocol.version=V3");
    if (!embedded) {
      if (isWindows()) {
        settings.dsbulkCmd = dsbulkDir.resolve("bin").resolve("dsbulk.cmd").toString();
      } else {
        settings.dsbulkCmd = dsbulkDir.resolve("bin").resolve("dsbulk").toString();
      }
    }
    return settings;
  }

  private void extractExternalDsbulk() throws IOException {
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
}
