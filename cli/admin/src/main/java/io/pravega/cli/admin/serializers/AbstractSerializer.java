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
package io.pravega.cli.admin.serializers;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.Serializer;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for serializers.
 */
public abstract class AbstractSerializer implements Serializer<String> {

    /**
     * Method to return the name of the metadata being serialized.
     *
     * @return A string representing the metadata that is dealt with by this serializer.
     */
    public abstract String getName();

    /**
     * Append the given field name-value in a user-friendly format to the StringBuilder.
     *
     * @param builder The StringBuilder to append to.
     * @param name    The name of the field.
     * @param value   The value of the field.
     */
    public static void appendField(StringBuilder builder, String name, String value) {
        builder.append(name).append("=").append(value).append(";");
    }

    /**
     * Parse the given string into a map of keys and values.
     *
     * @param stringData The string to parse
     * @return A map containing all the key-value pairs parsed from the string.
     */
    public static Map<String, String> parseStringData(String stringData) {
        Map<String, String> parsedData = new LinkedHashMap<>();
        Arrays.stream(stringData.split(";")).forEachOrdered(kv -> {
            List<String> pair = Arrays.asList(kv.split("="));
            Preconditions.checkArgument(pair.size() == 2, String.format("Incomplete key-value pair provided in %s", kv));
            if (!parsedData.containsKey(pair.get(0))) {
                parsedData.put(pair.get(0), pair.get(1));
            }
        });
        return parsedData;
    }

    /**
     * Checks if the given key exists in the map. If it exists, it returns the value corresponding to the key and removes
     * the key from the map.
     *
     * @param data The map to check in.
     * @param key  The key.
     * @return The value of the key if it exists.
     * @throws IllegalArgumentException if the key does not exist.
     */
    public static String getAndRemoveIfExists(Map<String, String> data, String key) {
        String value = data.remove(key);
        Preconditions.checkArgument(value != null, String.format("%s not provided.", key));
        return value;
    }
}
