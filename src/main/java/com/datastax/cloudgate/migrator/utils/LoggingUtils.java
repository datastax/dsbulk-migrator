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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Objects;
import org.slf4j.LoggerFactory;

public class LoggingUtils {

  public static final URL MIGRATOR_CONFIGURATION_FILE;

  static {
    try {
      MIGRATOR_CONFIGURATION_FILE =
          Objects.requireNonNull(
              System.getProperty("logback. configurationFile") == null
                  ? ClassLoader.getSystemResource("logback-migrator.xml")
                  : Paths.get(System.getProperty("logback. configurationFile")).toUri().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  public static void configureLogging(URL configurationFile) {
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    loggerContext.reset();
    JoranConfigurator configurator = new JoranConfigurator();
    try (InputStream configStream = configurationFile.openStream()) {
      configurator.setContext(loggerContext);
      configurator.doConfigure(configStream);
    } catch (Exception e) {
      throw new RuntimeException("Failed to configure logging", e);
    }
  }
}
