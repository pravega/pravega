/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import lombok.NonNull;

public class ConfigurationOptionsExtractor {

    public static String extractString(@NonNull String systemProperty, @NonNull String environmentVariable,
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

    public static Integer extractInt(@NonNull String systemProperty, @NonNull String environmentVariable,
                                     @NonNull Integer defaultValue) {
        String property = extractString(systemProperty, environmentVariable, defaultValue + "");
        Integer result = null;
        try {
            result = Integer.parseInt(property);
        } catch (NumberFormatException e) {
            // Ignore
            result = defaultValue;
        }
        return result;
    }
}
