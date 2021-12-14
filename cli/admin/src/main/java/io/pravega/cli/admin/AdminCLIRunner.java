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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;
import java.util.stream.Collectors;

import io.pravega.cli.admin.config.ConfigListCommand;
import io.pravega.cli.admin.utils.ConfigUtils;
import lombok.Cleanup;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the Pravega CLI.
 */
public final class AdminCLIRunner {
    private static final String CMD_HELP = "help";
    private static final String CMD_EXIT = "exit";

    /**
     * Main entry point for the Admin Tools Runner.
     * <p>
     * To speed up setup, create a admin-cli.properties file and put the following properties (at a minimum):
     * <p>
     * pravegaservice.container.count={number of containers}
     * pravegaservice.zk.connect.uri={host:port for ZooKeeper}
     * cli.controller.rest.uri={host:port for a Controller REST API endpoint}
     * bookkeeper.ledger.path={path in ZooKeeper where BookKeeper stores Ledger metadata}
     * <p>
     * This program can be executed in two modes. First, the "interactive mode", in which you may want to point to a
     * config file that contains the previous mandatory configuration parameters:
     * -Dpravega.configurationFile=admin-cli.properties
     *
     * If you don't want to use a config file, you still can load configuration properties dynamically using the command
     * "config set property=value".
     *
     * Second, this program can be executed in "batch mode" to execute a single command. To this end, you need to pass
     * as program argument the command to execute and as properties the configuration parameters (-D flag):
     * ./pravega-cli controller list-scopes -Dpravegaservice.container.count={value} -Dpravegaservice.zk.connect.uri={value} ...
     *
     * @param args Arguments.
     * @throws Exception If one occurred.
     */
    public static void main(String[] args) throws Exception {
        doMain(args);
        System.exit(0);
    }

    @VisibleForTesting
    public static void doMain(String[] args) throws IOException {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.ERROR);

        System.out.println("Pravega Admin CLI.\n");
        @Cleanup
        AdminCommandState state = new AdminCommandState();
        ConfigUtils.loadProperties(state);

        // Output loaded config.
        System.out.println("Initial configuration:");
        val initialConfigCmd = new ConfigListCommand(new CommandArgs(Collections.emptyList(), state));
        initialConfigCmd.execute();

        if (args == null || args.length == 0) {
            interactiveMode(state);
        } else {
            String commandLine = Arrays.stream(args).collect(Collectors.joining(" ", "", ""));
            processCommand(commandLine, state);
        }
    }

    private static void interactiveMode(AdminCommandState state) {
        // Continuously accept new commands as long as the user entered one.
        System.out.printf("%nType \"%s\" for list of commands, or \"%s\" to exit.%n", CMD_HELP, CMD_EXIT);
        @Cleanup
        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.print(System.lineSeparator() + "> ");
            String line = input.nextLine();
            processCommand(line, state);
        }
    }

    @VisibleForTesting
    public static void processCommand(String line, AdminCommandState state) {
        if (Strings.isNullOrEmpty(line.trim())) {
            return;
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

    private static void execCommand(Parser.Command pc, AdminCommandState state) {
        CommandArgs cmdArgs = new CommandArgs(pc.getArgs(), state);
        try {
            AdminCommand cmd = AdminCommand.Factory.get(pc.getComponent(), pc.getName(), cmdArgs);
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

    private static void printCommandSummary(AdminCommand.CommandDescriptor d) {
        System.out.println(String.format("\t%s %s %s: %s",
                d.getComponent(),
                d.getName(),
                Arrays.stream(d.getArgs()).map(AdminCLIRunner::formatArgName).collect(Collectors.joining(" ")),
                d.getDescription()));
    }

    @VisibleForTesting
    public static void printCommandDetails(Parser.Command command) {
        AdminCommand.CommandDescriptor d = AdminCommand.Factory.getDescriptor(command.getComponent(), command.getName());
        if (d == null) {
            printHelp(command);
            return;
        }

        printCommandSummary(d);
        for (AdminCommand.ArgDescriptor ad : d.getArgs()) {
            System.out.println(String.format("\t\t%s: %s", formatArgName(ad), ad.getDescription()));
        }
    }

    private static void printHelp(Parser.Command command) {
        Collection<AdminCommand.CommandDescriptor> commands;
        if (command == null) {
            // All commands.
            commands = AdminCommand.Factory.getDescriptors();
            System.out.println("All available commands:");
        } else {
            // Commands specific to a component.
            commands = AdminCommand.Factory.getDescriptors(command.getComponent());
            if (commands.isEmpty()) {
                System.out.println(String.format("No commands are available for component '%s'.", command.getComponent()));
            } else {
                System.out.println(String.format("All commands for component '%s':", command.getComponent()));
            }
        }

        commands.stream()
                .sorted(Comparator.comparing(AdminCommand.CommandDescriptor::getComponent).thenComparing(AdminCommand.CommandDescriptor::getName))
                .forEach(AdminCLIRunner::printCommandSummary);
    }

    private static String formatArgName(AdminCommand.ArgDescriptor ad) {
        return ad.isOptional() ? String.format("[%s]", ad.getName()) : String.format("<%s>", ad.getName());
    }
}
