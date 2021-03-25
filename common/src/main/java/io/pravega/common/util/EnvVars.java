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
import lombok.experimental.UtilityClass;

@UtilityClass
public final class EnvVars {

    /**
     * Utility function to (statically) read a integer from from an environmental variable. This is
     * useful to initialize static constants that would be useful to change without recompilation,
     * but which are not suitable to be made into config values. (IE because they are not intended
     * to be public).
     * 
     * @param variableName The name of the environmental variable to read.
     * @param defaultValue The default value to use if it is not set.
     * @return The value parsed (of the default if none was set)
     * @throws NumberFormatException if the variable was set but could not be parsed as an integer.
     */
    public static int readIntegerFromEnvVar(String variableName, int defaultValue) {
        return readIntegerFromString(System.getenv(variableName), variableName, defaultValue);
    }

    @VisibleForTesting
    static int readIntegerFromString(String string, String variableName, int defaultValue) {
        if (string != null) {
            try {
                return Integer.parseInt(string);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Enviromental variable " + variableName + " could not be parsed as an integer");
            }
        }
        return defaultValue;
    }
}
