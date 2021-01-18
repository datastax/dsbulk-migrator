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
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.cloudgate.migrator.live.SchemaLiveMigrator;
import com.datastax.cloudgate.migrator.settings.MigrationSettings;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SchemaMigrationIT extends ITBase {

  SchemaMigrationIT(BoundCluster origin, BoundCluster target) {
    super(origin, target);
  }

  @Test
  void should_migrate_with_embedded_dsbulk() throws IOException {
    // given
    MigrationSettings settings = createSettings(true);
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
    MigrationSettings settings = createSettings(false);
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

  @BeforeAll
  void setLogConfigurationFile() throws URISyntaxException {
    // required because the embedded mode resets the logging configuration after
    // each DSBulk invocation.
    System.setProperty(
        "logback. configurationFile",
        Paths.get(getClass().getResource("/logback-test.xml").toURI()).toString());
  }
}
