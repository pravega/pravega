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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.pravega.common.Exceptions;
import io.pravega.shared.NameUtils;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;

/**
 * Base class for any command to execute from the Admin tool.
 */
@RequiredArgsConstructor
abstract class Command {
    //region Private

    protected static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    @Getter
    @NonNull
    private final CommandArgs commandArgs;

    @VisibleForTesting
    @Getter
    @Setter
    private PrintStream out = System.out;

    //endregion

    //region Command Implementation

    /**
     * Executes the command with the arguments passed in via the Constructor. The command will allocate whatever resources
     * it needs to execute and will clean up after its execution completes (successful or not). The only expected side
     * effect may be the modification of the shared AdminCommandState that is passed in via the Constructor.
     *
     * @throws IllegalArgumentException If the arguments passed in via the Constructor are invalid.
     * @throws Exception                If the command failed to execute.
     */
    public abstract void execute() throws Exception;

    protected InteractiveConfig getConfig() {
        return getCommandArgs().getConfig();
    }

    protected URI getControllerUri() {
        return URI.create(getConfig().getControllerUri());
    }

    protected void output(String messageTemplate, Object... args) {
        this.out.println(String.format(messageTemplate, args));
    }

    protected boolean confirmContinue() {
        output("Do you want to continue?[yes|no]");
        Scanner s = new Scanner(System.in);
        String input = s.nextLine();
        return input.equals("yes");
    }

    //endregion

    //region Arguments

    protected void ensureArgCount(int expectedCount) {
        Preconditions.checkArgument(this.commandArgs.getArgs().size() == expectedCount, "Incorrect argument count.");
    }

    protected int getIntArg(int index) {
        return getArg(index, Integer::parseInt);
    }

    protected long getLongArg(int index) {
        return getArg(index, Long::parseLong);
    }

    protected boolean getBooleanArg(int index) {
        return getArg(index, Boolean::parseBoolean);
    }

    protected ScopedName getScopedNameArg(int index) {
        return getArg(index, scopedName -> {
            val parts = NameUtils.extractStreamNameTokens(scopedName);
            Preconditions.checkArgument(parts.size() == 2, "Invalid scoped Stream name. Expected format 'scope/stream': '%s'.", scopedName);
            return new ScopedName(parts.get(0), parts.get(1));
        });
    }

    protected <T> T getJsonArg(int index, Class<T> c) {
        String jsonArg = getArg(index);
        jsonArg = removeEnvelope(jsonArg, "{", "}");
        return GSON.fromJson(jsonArg, c);
    }

    protected String getArg(int index) {
        return removeEnvelope(this.commandArgs.getArgs().get(index), "\"", "\"");
    }

    private <T> T getArg(int index, Function<String, T> converter) {
        String s = null;
        try {
            s = getArg(index);
            return converter.apply(s);
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("Unexpected argument '%s' at position %d: %s.", s, index, ex.getMessage()));
        }
    }

    private String removeEnvelope(String s, String removeBeginning, String removeEnd) {
        if (s.length() > removeBeginning.length() + removeEnd.length()
                && s.startsWith(removeBeginning) && s.endsWith(removeEnd)) {
            s = s.substring(removeBeginning.length(), s.length() - removeEnd.length());
        }

        return s;
    }

    //endregion

    //region ScopedName

    @Data
    protected static class ScopedName {
        private final String scope;
        private final String name;

        @Override
        public String toString() {
            return NameUtils.getScopedStreamName(this.scope, this.name);
        }
    }

    //endregion

    //region Descriptors

    /**
     * Describes an argument.
     */
    @RequiredArgsConstructor
    @Getter
    public static class ArgDescriptor {
        private final String name;
        private final String description;
    }

    /**
     * Describes a Command.
     */
    @Getter
    static class CommandDescriptor {
        private final String component;
        private final String name;
        private final String description;
        private final ArgDescriptor[] args;

        public CommandDescriptor(String component, String name, String description, ArgDescriptor... args) {
            this.component = Exceptions.checkNotNullOrEmpty(component, "component");
            this.name = Exceptions.checkNotNullOrEmpty(name, "name");
            this.description = Exceptions.checkNotNullOrEmpty(description, "description");
            this.args = args;
        }
    }

    //endregion

    //region Factory

    /**
     * Helps create new Command instances.
     */
    public static class Factory {
        private static final Map<String, Map<String, CommandInfo>> COMMANDS = registerAll(
                ImmutableMap.<Supplier<CommandDescriptor>, CommandCreator>builder()
                        .put(ConfigCommand.List::descriptor, ConfigCommand.List::new)
                        .put(ConfigCommand.Set::descriptor, ConfigCommand.Set::new)
                        .put(ScopeCommand.Create::descriptor, ScopeCommand.Create::new)
                        .put(ScopeCommand.Delete::descriptor, ScopeCommand.Delete::new)
                        .put(StreamCommand.Create::descriptor, StreamCommand.Create::new)
                        .put(StreamCommand.Delete::descriptor, StreamCommand.Delete::new)
                        .put(StreamCommand.List::descriptor, StreamCommand.List::new)
                        .put(KeyValueTableCommand.Create::descriptor, KeyValueTableCommand.Create::new)
                        .put(KeyValueTableCommand.Delete::descriptor, KeyValueTableCommand.Delete::new)
                        .put(KeyValueTableCommand.Get::descriptor, KeyValueTableCommand.Get::new)
                        .put(KeyValueTableCommand.Put::descriptor, KeyValueTableCommand.Put::new)
                        .put(KeyValueTableCommand.Remove::descriptor, KeyValueTableCommand.Remove::new)
                        .put(KeyValueTableCommand.ListKeys::descriptor, KeyValueTableCommand.ListKeys::new)
                        .put(KeyValueTableCommand.ListEntries::descriptor, KeyValueTableCommand.ListEntries::new)
                        .build());

        /**
         * Gets a Collection of CommandDescriptors for all registered commands.
         *
         * @return A new Collection.
         */
        public static Collection<CommandDescriptor> getDescriptors() {
            ArrayList<CommandDescriptor> result = new ArrayList<>();
            COMMANDS.values().forEach(g -> g.values().forEach(c -> result.add(c.getDescriptor())));
            return result;
        }

        /**
         * Gets a Collection of CommandDescriptors for all registered commands for the given component.
         *
         * @param component The component to query.
         * @return A new Collection.
         */
        public static Collection<CommandDescriptor> getDescriptors(String component) {
            Map<String, CommandInfo> componentCommands = COMMANDS.getOrDefault(component, null);
            return componentCommands == null
                    ? Collections.emptyList()
                    : componentCommands.values().stream().map(CommandInfo::getDescriptor).collect(Collectors.toList());
        }

        /**
         * Gets a CommandDescriptor for the given commandArgs.
         *
         * @param component The name of the Component to get the descriptor for.
         * @param command   The name of the Command (within the Component) to get.
         * @return The CommandDescriptor for the given argument, or null if no such command is registered.
         */
        public static CommandDescriptor getDescriptor(String component, String command) {
            CommandInfo ci = getCommand(component, command);
            return ci == null ? null : ci.getDescriptor();
        }

        /**
         * Gets a new instance of a Command for the given commandArgs.
         *
         * @param component The name of the Component to get the Command for.
         * @param command   The name of the Command (within the Component) to get.
         * @param args      CommandArgs for the command.
         * @return A new instance of a Command base, already initialized with the command's commandArgs.
         */
        public static Command get(String component, String command, CommandArgs args) {
            CommandInfo ci = getCommand(component, command);
            return ci == null ? null : ci.getCreator().apply(args);
        }

        private static CommandInfo getCommand(String component, String command) {
            Map<String, CommandInfo> componentCommands = COMMANDS.getOrDefault(component, null);
            return componentCommands == null ? null : componentCommands.getOrDefault(command, null);
        }

        private static Map<String, Map<String, CommandInfo>> registerAll(Map<Supplier<CommandDescriptor>, CommandCreator> items) {
            val result = new HashMap<String, Map<String, CommandInfo>>();
            for (val e : items.entrySet()) {
                Command.CommandDescriptor d = e.getKey().get();
                Map<String, CommandInfo> componentCommands = result.getOrDefault(d.getComponent(), null);
                if (componentCommands == null) {
                    componentCommands = new HashMap<>();
                    result.put(d.getComponent(), componentCommands);
                }

                if (componentCommands.putIfAbsent(d.getName(), new CommandInfo(d, e.getValue())) != null) {
                    throw new IllegalArgumentException(String.format("A command is already registered for '%s'-'%s'.", d.getComponent(), d.getName()));
                }
            }
            return Collections.unmodifiableMap(result);
        }

        @Data
        private static class CommandInfo {
            private final CommandDescriptor descriptor;
            private final CommandCreator creator;
        }

        @FunctionalInterface
        private interface CommandCreator extends Function<CommandArgs, Command> {
        }
    }

    @Data
    private static class Tuple {
        private final String key;
        private final Object value;
    }

    private String objectToJSON(Object object) {
        try {
            return new ObjectMapper().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            System.err.println("Exception parsing object: " + e.getMessage());
        }
        return "";
    }

    //endregion
}
