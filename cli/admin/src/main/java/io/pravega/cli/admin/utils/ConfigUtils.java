/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.admin.utils;

import io.pravega.cli.admin.AdminCommandState;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class for configuration purposes.
 */
public class ConfigUtils {

    private static final String CONFIG_FILE_PROPERTY_NAME = "pravega.configurationFile";
    private static final String PRAVEGA_SERVICE_PROPERTY_NAME = "pravegaservice";
    private static final String CLI_PROPERTY_NAME = "cli";
    private static final String BOOKKEEPER_PROPERTY_NAME = "bookkeeper";

    public static void loadProperties(AdminCommandState state) {
        Properties pravegaProperties = new Properties();
        // First, load the properties from file, if any.
        try (InputStream input = new FileInputStream(System.getProperty(CONFIG_FILE_PROPERTY_NAME))) {
            pravegaProperties.load(input);
        } catch (Exception e) {
            System.err.println("Exception reading input properties file: " + e.getMessage());
            pravegaProperties.clear();
        }

        // Second, load properties from command line if any.
        for (String propertyName: System.getProperties().stringPropertyNames()) {
            if (propertyName.startsWith(PRAVEGA_SERVICE_PROPERTY_NAME)
                    || propertyName.startsWith(CLI_PROPERTY_NAME)
                    || propertyName.startsWith(BOOKKEEPER_PROPERTY_NAME)) {
                pravegaProperties.setProperty(propertyName, System.getProperties().getProperty(propertyName));
            }
        }
        state.getConfigBuilder().include(pravegaProperties);
    }
}
