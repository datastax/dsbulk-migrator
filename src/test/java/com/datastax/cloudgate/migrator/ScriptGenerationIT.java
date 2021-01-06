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

import static com.datastax.oss.dsbulk.tests.utils.FileUtils.readFile;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.cloudgate.migrator.script.SchemaScriptGenerator;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class ScriptGenerationIT extends ITBase {

  ScriptGenerationIT(BoundCluster origin, BoundCluster target) {
    super(origin, target);
  }

  @Test
  void should_should_generate_scripts() throws IOException {
    // given
    MigrationSettings settings = createSettings(false);
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
}
