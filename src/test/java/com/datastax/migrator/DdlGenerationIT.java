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

import static com.datastax.oss.dsbulk.tests.utils.FileUtils.readFile;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.migrator.ddl.DdlGenerationSettings;
import com.datastax.migrator.ddl.SchemaDdlGenerator;
import com.datastax.migrator.settings.ExportSettings.ExportClusterInfo;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DdlGenerationIT extends SimulacronITBase {

  Path dataDir;
  Path keyspacesFile;
  Path tablesFile;

  DdlGenerationIT(BoundCluster origin, BoundCluster target) {
    super(origin, target);
  }

  @Test
  void should_generate_generic_ddl_files() throws IOException {
    // given
    DdlGenerationSettings settings = createSettings();
    // when
    SchemaDdlGenerator generator = new SchemaDdlGenerator(settings);
    generator.generate();
    // then
    assertThat(dataDir).exists().isDirectory();
    assertThat(keyspacesFile).exists().isRegularFile();
    assertThat(tablesFile).exists().isRegularFile();
    assertThat(readFile(keyspacesFile))
        .contains(
            "CREATE KEYSPACE \"test\" WITH replication = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND durable_writes = true;");
    assertThat(readFile(tablesFile))
        .contains(
            "CREATE TABLE \"test\".\"t1\" ( \"pk\" int, \"cc\" int, \"v\" int, PRIMARY KEY (\"pk\", \"cc\") )")
        .contains("default_time_to_live")
        .contains("bloom_filter_fp_chance")
        .contains("gc_grace_seconds")
        .contains("caching")
        .contains("compaction");
  }

  @Test
  void should_generate_astra_compatible_ddl_files() throws IOException {
    // given
    DdlGenerationSettings settings = createSettings();
    settings.optimizeForAstra = true;
    // when
    SchemaDdlGenerator generator = new SchemaDdlGenerator(settings);
    generator.generate();
    // then
    assertThat(dataDir).exists().isDirectory();
    assertThat(keyspacesFile).exists().isRegularFile();
    assertThat(tablesFile).exists().isRegularFile();
    assertThat(readFile(keyspacesFile))
        .contains(
            "CREATE KEYSPACE \"test\" WITH replication = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND durable_writes = true;");
    assertThat(readFile(tablesFile))
        .contains(
            "CREATE TABLE \"test\".\"t1\" ( \"pk\" int, \"cc\" int, \"v\" int, PRIMARY KEY (\"pk\", \"cc\") )")
        .contains("default_time_to_live")
        .doesNotContain("bloom_filter_fp_chance")
        .doesNotContain("gc_grace_seconds")
        .doesNotContain("caching")
        .doesNotContain("compaction");
  }

  @BeforeEach
  void createTempDirs() throws IOException {
    dataDir = Files.createTempDirectory("data");
    keyspacesFile = dataDir.resolve("dsbulk-migrator-ddl-keyspaces.cql");
    tablesFile = dataDir.resolve("dsbulk-migrator-ddl-tables.cql");
  }

  @AfterEach
  void deleteTempDirs() {
    if (dataDir != null && Files.exists(dataDir)) {
      FileUtils.deleteDirectory(dataDir);
    }
  }

  private DdlGenerationSettings createSettings() {
    DdlGenerationSettings settings = new DdlGenerationSettings();
    settings.dataDir = dataDir;
    settings.clusterInfo = new ExportClusterInfo();
    settings.clusterInfo.hostsAndPorts =
        Collections.singletonList(HostAndPort.fromString(originHost));
    settings.clusterInfo.hostsAndPorts =
        Collections.singletonList(HostAndPort.fromString(targetHost));
    return settings;
  }
}
