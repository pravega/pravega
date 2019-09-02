/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Strings;
import io.pravega.segmentstore.server.host.admin.commands.AdminCommandState;
import io.pravega.segmentstore.server.host.admin.commands.Command;
import io.pravega.segmentstore.server.host.admin.commands.CommandArgs;
import io.pravega.segmentstore.server.host.admin.commands.ConfigListCommand;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Scanner;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the Admin tools.
 */
public final class AdminRunner {
    private static final String CMD_HELP = "help";
    private static final String CMD_EXIT = "exit";

    /**
     * Main entry point for the Admin Tools Runner.
     * <p>
     * To speed up setup, create a config.properties file and put the following properties (at a minimum):
     * <p>
     * pravegaservice.containerCount={number of containers}
     * pravegaservice.zkURL={host:port for ZooKeeper}
     * bookkeeper.bkLedgerPath={path in ZooKeeper where BookKeeper stores Ledger metadata}
     * bookkeeper.zkMetadataPath={path in ZooKeeper where Pravega stores BookKeeperLog metadata}
     * <p>
     * Then invoke this program with:
     * -Dpravega.configurationFile=config.properties
     *
     * @param args Arguments.
     * @throws Exception If one occurred.
     */
    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.ERROR);

        System.out.println("Pravega Admin Tools.\n");
        @Cleanup
        AdminCommandState state = new AdminCommandState();

        // Output loaded config.
        System.out.println("Initial configuration:");
        val initialConfigCmd = new ConfigListCommand(new CommandArgs(Collections.emptyList(), state));
        initialConfigCmd.execute();

        // Continuously accept new commands as long as the user entered one.
        System.out.println(String.format("%nType \"%s\" for list of commands, or \"%s\" to exit.", CMD_HELP, CMD_EXIT));
        @Cleanup
        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.print(System.lineSeparator() + "> ");
            String line = input.nextLine();
            if (Strings.isNullOrEmpty(line.trim())) {
                continue;
            }

            Parser.Command pc = Parser.parse(line);
            switch (pc.getComponent()) {
                case CMD_HELP:
                    printHelp(null);
                    break;
                case CMD_EXIT:
                    System.exit(0);
                    break;
                default:
                    execCommand(pc, state);
                    break;
            }
        }
    }

    private static void execCommand(Parser.Command pc, AdminCommandState state) {
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
            ex.printStackTrace(System.out);
        }
    }

    private static void printCommandSummary(Command.CommandDescriptor d) {
        System.out.println(String.format("\t%s %s %s: %s",
                d.getComponent(),
                d.getName(),
                Arrays.stream(d.getArgs()).map(AdminRunner::formatArgName).collect(Collectors.joining(" ")),
                d.getDescription()));
    }

    private static void printCommandDetails(Parser.Command command) {
        Command.CommandDescriptor d = Command.Factory.getDescriptor(command.getComponent(), command.getName());
        if (d == null) {
            printHelp(command);
            return;
        }

        printCommandSummary(d);
        for (Command.ArgDescriptor ad : d.getArgs()) {
            System.out.println(String.format("\t\t%s: %s", formatArgName(ad), ad.getDescription()));
        }
    }

    private static void printHelp(Parser.Command command) {
        Collection<Command.CommandDescriptor> commands;
        if (command == null) {
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
