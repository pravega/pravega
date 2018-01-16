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

import com.google.common.base.Strings;
import io.pravega.segmentstore.server.host.admin.commands.Command;
import io.pravega.segmentstore.server.host.admin.commands.CommandArgs;
import io.pravega.segmentstore.server.host.admin.commands.State;
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
        State state = new State();
        while (!Strings.isNullOrEmpty(line = input.nextLine())) {
            Parser.Command pc = Parser.parse(line);
            CommandArgs cmdArgs = new CommandArgs(pc.getArgs(), state);
            try {
                Command cmd = Command.Factory.get(pc.getComponent(), pc.getName(), cmdArgs);
                if (cmd == null) {
                    // No command was found.
                    printHelp(pc);
                } else {
                    cmd.execute();
                }
            } catch (IllegalArgumentException ex) {
                // We found a command, but had the wrong arguments to it.
                System.out.println("Bad command syntax: " + ex.getMessage());
                printCommandDetails(pc);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            System.out.print(System.lineSeparator() + "> ");
        }
    }

    private static void printCommandSummary(Command.CommandDescriptor d) {
        System.out.println(String.format("%s %s %s: %s",
                d.getComponent(),
                d.getName(),
                String.join(" ", d.getArgs().stream().map(AdminRunner::formatArgName).collect(Collectors.toList())),
                d.getDescription()));
    }

    private static void printCommandDetails(Parser.Command command) {
        Command.CommandDescriptor d = Command.Factory.getDescriptor(command.getComponent(), command.getName());
        if (d == null) {
            printHelp(command);
            return;
        }

        printCommandSummary(d);
        d.getArgs().forEach(ad -> System.out.println(String.format("\t%s: %s", formatArgName(ad), ad.getDescription())));
    }

    private static void printHelp(Parser.Command command) {
        Collection<Command.CommandDescriptor> commands;
        if (command == null || command.getComponent().equals("help")) {
            // All commands.
            commands = Command.Factory.getDescriptors();
            System.out.println("All available commands:");
        } else {
            // Commands specific to a component.
            commands = Command.Factory.getDescriptors(command.getComponent());
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

    private static String formatArgName(Command.ArgDescriptor ad) {
        return String.format("<%s>", ad.getName());
    }
}
