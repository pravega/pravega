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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializerUtils {

    public static void addField(StringBuilder builder, String name, String value) {
        builder.append(name).append(": ").append(value).append(";\n");
    }

    public static Map<String, String> parseStringData(String stringData) {
        Map<String, String> parsedData = new HashMap<>();
        List<String> fields = Arrays.asList(stringData.split(";"));
        fields.forEach(kv -> {
            List<String> pair = Arrays.asList(kv.split("="));
            assert pair.size() == 2;
            parsedData.put(pair.get(0), pair.get(1));
        });
        return parsedData;
    }

    public static String getAndRemoveIfExists(Map<String, String> data, String key) {
        if (!data.containsKey(key)) {
            throw new IllegalArgumentException(String.format("%s not provided.", key));
        }
        return data.remove(key);
    }
}
