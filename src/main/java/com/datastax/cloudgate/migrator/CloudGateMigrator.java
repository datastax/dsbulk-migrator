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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.cloudgate.migrator.live.SchemaLiveMigrator;
import com.datastax.cloudgate.migrator.script.SchemaScriptGenerator;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.LoggerFactory;

public class CloudGateMigrator {

  public static void main(String[] args) throws Exception {
    configureLogging(SchemaLiveMigrator.class.getResource("/logback-migrator.xml"));
    List<String> arguments = new ArrayList<>(Arrays.asList(args));
    String command = arguments.remove(0);
    MigrationSettings settings = new MigrationSettings(arguments);
    if (command.equals("migrate-live")) {
      new SchemaLiveMigrator(settings).migrate();
    } else if (command.equals("generate-script")) {
      new SchemaScriptGenerator(settings).generate();
    } else {
      throw new IllegalArgumentException("Unknown command: " + command);
    }
  }

  public static void configureLogging(URL configurationFile) throws IOException, JoranException {
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    loggerContext.reset();
    JoranConfigurator configurator = new JoranConfigurator();
    try (InputStream configStream = configurationFile.openStream()) {
      configurator.setContext(loggerContext);
      configurator.doConfigure(configStream);
    }
  }
}
