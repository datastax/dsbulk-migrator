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

import com.datastax.migrator.script.SchemaScriptGenerator;
import com.datastax.migrator.script.ScriptGenerationSettings;
import com.datastax.migrator.settings.ExportSettings.ExportClusterInfo;
import com.datastax.migrator.settings.ImportSettings.ImportClusterInfo;
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

class ScriptGenerationIT extends SimulacronITBase {

  Path dataDir;
  Path logsDir;
  Path exportScript;
  Path importScript;

  ScriptGenerationIT(BoundCluster origin, BoundCluster target) {
    super(origin, target);
  }

  @Test
  void should_should_generate_scripts() throws IOException {
    // given
    ScriptGenerationSettings settings = createSettings();
    // when
    SchemaScriptGenerator generator = new SchemaScriptGenerator(settings);
    generator.generate();
    // then
    assertThat(dataDir).exists().isDirectory();
    assertThat(exportScript).exists().isRegularFile();
    assertThat(importScript).exists().isRegularFile();
    assertThat(readFile(exportScript))
        .contains(
            "echo 'Exporting table test.t1'",
            "  \"${dsbulk_cmd}\" unload \\",
            "    -query \"SELECT pk, cc, v, WRITETIME(v) AS v_writetime, TTL(v) AS v_ttl FROM test.t1\"");
    assertThat(readFile(importScript))
        .contains(
            "echo 'Importing table test.t1'",
            "  \"${dsbulk_cmd}\" load \\",
            "    -query \"INSERT INTO test.t1 (pk, cc, v) VALUES (:pk, :cc, :v) USING TIMESTAMP :v_writetime AND TTL :v_ttl\"");
  }

  @BeforeEach
  void createTempDirs() throws IOException {
    dataDir = Files.createTempDirectory("data");
    logsDir = Files.createTempDirectory("logs");
    exportScript = dataDir.resolve("dsbulk-migrator-export.sh");
    importScript = dataDir.resolve("dsbulk-migrator-import.sh");
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

  private ScriptGenerationSettings createSettings() {
    ScriptGenerationSettings settings = new ScriptGenerationSettings();
    settings.dataDir = dataDir;
    settings.exportSettings.clusterInfo = new ExportClusterInfo();
    settings.importSettings.clusterInfo = new ImportClusterInfo();
    settings.exportSettings.clusterInfo.hostsAndPorts =
        Collections.singletonList(HostAndPort.fromString(originHost));
    settings.importSettings.clusterInfo.hostsAndPorts =
        Collections.singletonList(HostAndPort.fromString(targetHost));
    settings.importSettings.clusterInfo.protocolVersion = "V4";
    settings.exportSettings.clusterInfo.protocolVersion = "V4";
    settings.dsbulkLogDir = logsDir;
    return settings;
  }
}
