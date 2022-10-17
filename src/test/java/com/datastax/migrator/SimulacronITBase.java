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

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Column;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Keyspace;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Table;
import com.datastax.oss.dsbulk.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SimulacronConfig(dseVersion = "")
abstract class SimulacronITBase {

  final BoundCluster origin;
  final BoundCluster target;

  final String originHost;
  final String targetHost;

  SimulacronITBase(BoundCluster origin, BoundCluster target) {
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

    LinkedHashMap<String, String> columnTypes =
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
                    new LinkedHashMap<>(),
                    columnTypes))
            .then(new SuccessResult(Collections.emptyList(), new LinkedHashMap<>())));
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
  static <K, V> LinkedHashMap<K, V> map(Object... keysAndValues) {
    LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length - 1; i += 2) {
      map.put(keysAndValues[i], keysAndValues[i + 1]);
    }
    return (LinkedHashMap<K, V>) map;
  }
}
