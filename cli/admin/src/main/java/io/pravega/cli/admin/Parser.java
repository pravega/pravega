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
package io.pravega.cli.admin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Helps parse Strings into Commands.
 */
public class Parser {

    private static final String SCANNER_PATTERN = "[^\"\\s]+|\"(\\\\.|[^\\\\\"])*\"";

    /**
     * Parses the given String into a Command, separating elements by spaces, and treating characters between double quotes(")
     * as a single element. The first element is the Command Component, the second is the Command Name and the rest will
     * be gathered as an ordered list of arguments.
     *
     * @param s The string to parse.
     * @return A new instance of the Command class.
     */
    public static Command parse(String s) {
        @Cleanup
        Scanner scanner = new Scanner(s);
        String component = scanner.findInLine(SCANNER_PATTERN);
        String command = scanner.findInLine(SCANNER_PATTERN);
        ArrayList<String> args = new ArrayList<>();
        String arg;
        while ((arg = scanner.findInLine(SCANNER_PATTERN)) != null) {
            args.add(arg);
        }

        return new Command(component, command, Collections.unmodifiableList(args));
    }

    /**
     * Represents a parsed Command.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    public static class Command {
        private final String component;
        private final String name;
        private final List<String> args;

        @Override
        public String toString() {
            return String.format("%s %s (%s)", this.component, this.name, String.join(", ", this.args));
        }
    }
}
