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
package com.datastax.cloudgate.migrator.settings;

import com.datastax.cloudgate.migrator.utils.VersionUtils;
import picocli.CommandLine.IVersionProvider;

public class VersionProvider implements IVersionProvider {

  @Override
  public String[] getVersion() {
    String version = VersionUtils.getVersionString();
    return new String[] {
      "CloudGate Migrator " + version,
      "Java version: ${java.version}, ${java.vendor} (${java.vm.name} version ${java.vm.version})",
      "OS: ${os.name} version ${os.version} (${os.arch})",
      "(C) DataStax"
    };
  }
}
