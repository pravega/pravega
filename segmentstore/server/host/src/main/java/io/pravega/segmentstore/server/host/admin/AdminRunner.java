/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin;

import java.util.Collection;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * Main entry point for the Admin tools.
 */
public final class AdminRunner {
    public static void main(String[] args) {
        System.out.println("Admin tools. Type \"help\" for list of commands.");
        Scanner input = new Scanner(System.in);
        System.out.print(System.lineSeparator() + "> ");
        String line;
        while ((line = input.nextLine()) != null) {
            Parser.Command parsedCommand = Parser.parse(line);
            CommandBase cmd = null;
            try {
                cmd = Commands.get(parsedCommand);
                if (cmd == null) {
                    // No command was found.
                    printHelp(parsedCommand);
                }
            } catch (IllegalArgumentException ex) {
                // We found a command, but had the wrong arguments to it.
                System.out.println("Bad command syntax: " + ex.getMessage());
                printCommandDetails(parsedCommand);
            }

            if (cmd != null) {
                try {
                    cmd.execute();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            System.out.print(System.lineSeparator() + "> ");
        }
    }

    private static void printCommandSummary(CommandBase.CommandDescriptor d) {
        System.out.println(String.format("%s %s %s: %s",
                d.getComponent(),
                d.getName(),
                String.join(" ", d.getArgs().stream().map(AdminRunner::formatArgName).collect(Collectors.toList())),
                d.getDescription()));
    }

    private static void printCommandDetails(Parser.Command command) {
        CommandBase.CommandDescriptor d = Commands.getDescriptor(command);
        if (d == null) {
            printHelp(command);
            return;
        }

        printCommandSummary(d);
        d.getArgs().forEach(ad -> System.out.println(String.format("\t%s: %s", formatArgName(ad), ad.getDescription())));
    }

    private static void printHelp(Parser.Command command) {
        Collection<CommandBase.CommandDescriptor> commands;
        if (command == null || command.getComponent().equals("help")) {
            // All commands.
            commands = Commands.getDescriptors();
            System.out.println("All available commands:");
        } else {
            // Commands specific to a component.
            commands = Commands.getDescriptors(command.getComponent());
            if (commands.isEmpty()) {
                System.out.println(String.format("No commands are available for component '%s'.", command.getComponent()));
            } else {
                System.out.println(String.format("All commands for component '%s':", command.getComponent()));
            }
        }

        commands.stream()
                .sorted((d1, d2) -> {
                    int c = d1.getComponent().compareTo(d2.getComponent());
                    if (c == 0) {
                        c = d1.getName().compareTo(d2.getName());
                    }
                    return c;
                })
                .forEach(AdminRunner::printCommandSummary);
    }

    private static String formatArgName(CommandBase.ArgDescriptor ad) {
        return String.format("<%s>", ad.getName());
    }
}
