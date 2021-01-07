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

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class VersionUtils {

  public static String getVersionString() {
    try (InputStreamReader reader =
        new InputStreamReader(
            VersionUtils.class.getResourceAsStream("/migrator.properties"),
            StandardCharsets.UTF_8)) {
      Properties props = new Properties();
      props.load(reader);
      String groupId = props.getProperty("migrator.groupId");
      String artifactId = props.getProperty("migrator.artifactId");
      String version = props.getProperty("migrator.version");
      return String.format("version %s (%s:%s)", version, groupId, artifactId);
    } catch (IOException e) {
      return "";
    }
  }
}
