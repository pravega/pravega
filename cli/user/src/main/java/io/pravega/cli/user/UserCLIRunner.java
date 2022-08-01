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
package io.pravega.cli.user;

import ch.qos.logback.classic.LoggerContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.cli.user.config.ConfigCommand;
import io.pravega.cli.user.config.InteractiveConfig;
import lombok.Cleanup;
import lombok.val;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Interactive CLI Demo Tool for Pravega.
 */
public class UserCLIRunner {
    private static final String CMD_HELP = "help";
    private static final String CMD_EXIT = "exit";

    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    public static void main(String[] args) {
        doMain(args, System.in);
        System.exit(0);
    }

    @VisibleForTesting
    public static void doMain(String[] args, InputStream interactiveStream) {
        System.out.println("Pravega User CLI Tool.");
        System.out.println("\tUsage instructions: https://github.com/pravega/pravega/wiki/Pravega-User-CLI\n");
        val config = InteractiveConfig.getDefault(System.getenv());
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLoggerList().get(0).setLevel(config.getLogLevel());

        // Output loaded config.
        System.out.println("Initial configuration:");
        val initialConfigCmd = new ConfigCommand.List(new CommandArgs(Collections.emptyList(), config));
        initialConfigCmd.execute();

        if (args == null || args.length == 0) {
            interactiveMode(config, loggerContext, interactiveStream);
        } else {
            String commandLine = Arrays.stream(args).collect(Collectors.joining(" ", "", ""));
            processCommand(commandLine, config);
        }
    }

    private static void interactiveMode(InteractiveConfig config, LoggerContext loggerContext, InputStream interactiveStream) {
        // Continuously accept new commands as long as the user entered one.
        System.out.println(String.format("%nType \"%s\" for list of commands, or \"%s\" to exit.", CMD_HELP, CMD_EXIT));
        @Cleanup
        Scanner input = new Scanner(interactiveStream);
        while (RUNNING.get()) {
            System.out.print(System.lineSeparator() + "> ");
            String line = input.nextLine();

            loggerContext.getLoggerList().get(0).setLevel(config.getLogLevel());
            processCommand(line, config);
        }
    }

    @VisibleForTesting
    public static void processCommand(String line, InteractiveConfig config) {
        if (Strings.isNullOrEmpty(line.trim())) {
            return;
        }

        Parser.Command pc = Parser.parse(line, config);
        switch (pc.getComponent()) {
            case CMD_HELP:
                printHelp(null);
                break;
            case CMD_EXIT:
                RUNNING.set(false);
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
            System.out.println("\nCommand usage:");
            printCommandDetails(pc);
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }

    private static void printCommandSummary(Command.CommandDescriptor d) {
        System.out.println(String.format("\t%s %s %s: %s",
                d.getComponent(),
                d.getName(),
                d.getArgs().stream().map(UserCLIRunner::formatArgInline).collect(Collectors.joining(" ")),
                d.getDescription()));
    }

    @VisibleForTesting
    public static void printCommandDetails(Parser.Command command) {
        Command.CommandDescriptor d = Command.Factory.getDescriptor(command.getComponent(), command.getName());
        if (d == null) {
            printHelp(command);
            return;
        }

        printCommandSummary(d);
        for (Command.ArgDescriptor ad : d.getArgs()) {
            System.out.println(String.format("\t  - %s: %s", formatArgTable(ad), ad.getDescription()));
        }

        if (d.getSyntaxExamples().size() > 0) {
            System.out.println("Examples:");
            for (val se : d.getSyntaxExamples()) {
                System.out.println(String.format("\t   %s %s %s", d.getComponent(), d.getName(), se.getArgs()));
                System.out.println(String.format("\t    -> %s", se.getDescription()));
            }
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
                .forEach(UserCLIRunner::printCommandSummary);
    }

    private static String formatArgInline(Command.ArgDescriptor ad) {
        return ad.getName();
    }

    private static String formatArgTable(Command.ArgDescriptor ad) {
        return Strings.padEnd(ad.getName(), 20, ' ');
    }

}
