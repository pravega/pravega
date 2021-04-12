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
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A utility class for processing configuration items specified via system properties and/ environment variables.
 */
@Slf4j
public class ConfigurationOptionsExtractor {

    /**
     * Extracts a configuration value as String from the specified from system property or environment variable. If
     * both are specified, the value from system property is returned. If neither are specified, it returns
     * the {code defaultValue}.
     *
     * @param systemProperty the system property to extract the configuration value from
     * @param environmentVariable the environment variable to extract the configuration value from
     * @param defaultValue the value to be returned if neither {@code systemProperty} or {@code environmentVariable} is
     *                     specified
     * @return the specified or default configuration value
     */
    @VisibleForTesting
    static String extractString(@NonNull String systemProperty, @NonNull String environmentVariable,
                                       @NonNull String defaultValue) {
        String result = null;
        String valueFromSystemProperty = System.getProperty(systemProperty);
        if (valueFromSystemProperty != null && !valueFromSystemProperty.trim().equals("")) {
            result = valueFromSystemProperty;
        } else {
            String valueFromEnv = System.getenv(environmentVariable);
            if (valueFromEnv != null && valueFromEnv.trim().equals("")) {
                result = valueFromEnv;
            } else {
                result = defaultValue;
            }
        }
        return result;
    }

    /**
     * Extracts a configuration value as integer from the specified from system property or environment variable.
     * If both are specified, the value from system property is returned. If neither are specified, it returns
     * the {code defaultValue}. If the configuration value isn't an integer, it returns the default value.
     *
     * @param systemProperty the system property to extract the configuration value from
     * @param environmentVariable the environment variable to extract the configuration value from
     * @param defaultValue the value to be returned if neither {@code systemProperty} or {@code environmentVariable} is
     *                     specified
     * @return the specified or default configuration value
     */
    public static Integer extractInt(@NonNull String systemProperty, @NonNull String environmentVariable,
                                     @NonNull Integer defaultValue) {
        String property = extractString(systemProperty, environmentVariable, String.valueOf(defaultValue));
        Integer result = null;
        try {
            result = Integer.parseInt(property);
        } catch (NumberFormatException e) {
            log.warn("Value of the system property {} or environment variable {} is not an integer: {}",
                    systemProperty, environmentVariable, property);
            // Ignore
            result = defaultValue;
        }
        return result;
    }
}
