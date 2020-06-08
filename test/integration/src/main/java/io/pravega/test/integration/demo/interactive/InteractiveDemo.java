/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo.interactive;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * config list
 * config set controller-host=XYZ controller-port=XYZ
 *
 * stream create XYZ
 * stream delete XYZ
 * stream append XYZ {JSON}
 * stream txn begin XYZ
 * stream txn commit XYZ TXN-id
 * stream txn abort XYZ TXN-id
 * stream read XYZ
 *
 * kvt create XYZ
 * kvt delete XYZ
 * kvt get XYZ {JSON: [KF], k1, ..., kn} -> JSON {K, Ver, Value}
 * kvt put XYZ {JSON: [KF], {k1, ver1, val1}, ...}
 * kvt remove XYZ {JSON: [KF], {k1, ver1}, ...}
 * kvt list-keys XYZ KF [refresh-rate]
 * kvt list-entries XYZ KF [refresh-rate]
 */
public class InteractiveDemo {
    private static final String CMD_HELP = "help";
    private static final String CMD_EXIT = "exit";

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.ERROR);

        System.out.println("Pravega CLI.\n");
        val config = InteractiveConfig.getDefault();

        // Output loaded config.
        System.out.println("Initial configuration:");
        val initialConfigCmd = new ConfigCommand.List(new CommandArgs(Collections.emptyList(), config));
        initialConfigCmd.execute();

        if (args == null || args.length == 0) {
            interactiveMode(config);
        } else {
            String commandLine = Arrays.stream(args).collect(Collectors.joining(" ", "", ""));
            processCommand(commandLine, config);
        }
        System.exit(0);
    }

    private static void interactiveMode(InteractiveConfig config) {
        // Continuously accept new commands as long as the user entered one.
        System.out.println(String.format("%nType \"%s\" for list of commands, or \"%s\" to exit.", CMD_HELP, CMD_EXIT));
        @Cleanup
        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.print(System.lineSeparator() + "> ");
            String line = input.nextLine();
            processCommand(line, config);
        }
    }


    private static void processCommand(String line, InteractiveConfig config) {
        if (Strings.isNullOrEmpty(line.trim())) {
            return;
        }

        Parser.Command pc = Parser.parse(line, config);
        switch (pc.getComponent()) {
            case CMD_HELP:
                printHelp(null);
                break;
            case CMD_EXIT:
                System.exit(0);
                break;
            default:
                execCommand(pc);
                break;
        }
    }


    private static void execCommand(Parser.Command pc) {
        try {
            Command cmd = Command.Factory.get(pc.getComponent(), pc.getName(), pc.getArgs());
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
                Arrays.stream(d.getArgs()).map(InteractiveDemo::formatArgName).collect(Collectors.joining(" ")),
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
                .sorted(Comparator.comparing(Command.CommandDescriptor::getComponent).thenComparing(Command.CommandDescriptor::getName))
                .forEach(InteractiveDemo::printCommandSummary);
    }

    private static String formatArgName(Command.ArgDescriptor ad) {
        return String.format("<%s>", ad.getName());
    }

}
