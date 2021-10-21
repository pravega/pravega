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
package io.pravega.cli.user.utils;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Output formatter.
 */
public abstract class Formatter {

    public abstract List<String> format(String... items);

    public abstract String separator();

    /**
     * Outputs in table-like format, with columns.
     */
    @RequiredArgsConstructor
    public static class TableFormatter extends Formatter {
        static final char SEPARATOR = '-';
        @NonNull
        private final int[] columnLengths;

        @Override
        public List<String> format(String... items) {
            String[] currentLine = Arrays.copyOf(items, Math.min(items.length, this.columnLengths.length));
            List<String> result = new ArrayList<>();
            do {
                String[] nextLine = null;
                for (int i = 0; i < currentLine.length; i++) {
                    String s = Strings.nullToEmpty(currentLine[i]);
                    int maxLen = this.columnLengths[i];
                    if (s.length() > maxLen) {
                        if (nextLine == null) {
                            nextLine = new String[currentLine.length];
                        }
                        nextLine[i] = s.substring(maxLen);
                        s = s.substring(0, maxLen);
                    } else {
                        s = Strings.padEnd(s, maxLen, ' ');
                    }
                    currentLine[i] = s;
                }
                result.add(String.join(" | ", currentLine));

                currentLine = nextLine;
            } while (currentLine != null);
            assert result.size() > 0;
            return result;
        }

        @Override
        public String separator() {
            return format(Arrays.stream(this.columnLengths).boxed().map(l -> Strings.padEnd("", l, SEPARATOR)).toArray(String[]::new)).get(0);
        }
    }

    /**
     * Outputs in JSON format.
     */
    public static class JsonFormatter extends Formatter {
        public static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

        @Override
        public List<String> format(String... items) {
            if (items.length == 1) {
                return Collections.singletonList(GSON.toJson(items[0]));
            } else {
                return Collections.singletonList(GSON.toJson(items));
            }
        }

        @Override
        public String separator() {
            return "";
        }
    }
}
