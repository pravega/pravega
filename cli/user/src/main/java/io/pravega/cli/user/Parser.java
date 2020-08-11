/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user;

import io.pravega.cli.user.config.InteractiveConfig;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

final class Parser {

    private static final String MATCH_ESCAPED_DOUBLE_QUOTES = "[^\"\\s]+|\"(\\\\.|[^\\\\\"])*\"";
    private static final String MATCH_BRACES = "(^\\{\\s]+)|(\\{).+\\}";
    private static final String SCANNER_PATTERN = String.format("(%s)|(%s)", MATCH_BRACES, MATCH_ESCAPED_DOUBLE_QUOTES);

    /**
     * Parses the given String into a Command, separating elements by spaces, and treating characters between double quotes(")
     * as a single element. The first element is the Command Component, the second is the Command Name and the rest will
     * be gathered as an ordered list of arguments.
     *
     * @param s The string to parse.
     * @return A new instance of the Command class.
     */
    static Command parse(String s, InteractiveConfig config) {
        @Cleanup
        Scanner scanner = new Scanner(s);
        String component = scanner.findInLine(SCANNER_PATTERN);
        String command = scanner.findInLine(SCANNER_PATTERN);
        ArrayList<String> args = new ArrayList<>();
        String arg;
        while ((arg = scanner.findInLine(SCANNER_PATTERN)) != null) {
            args.add(arg);
        }

        return new Command(component, command, new CommandArgs(Collections.unmodifiableList(args), config));
    }

    /**
     * Represents a parsed Command.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    static class Command {
        private final String component;
        private final String name;
        private final CommandArgs args;

        @Override
        public String toString() {
            return String.format("%s %s (%s)", this.component, this.name, this.args);
        }
    }
}