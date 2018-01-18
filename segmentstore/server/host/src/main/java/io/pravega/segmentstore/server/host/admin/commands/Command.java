/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin.commands;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.server.store.ServiceConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Base class for any command to execute from the Admin tool.
 */
public abstract class Command {
    //region Private

    @Getter(AccessLevel.PROTECTED)
    private final CommandArgs commandArgs;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    Command(CommandArgs args) {
        this.commandArgs = Preconditions.checkNotNull(args, "commandArgs");
    }

    //endregion

    //region Command Implementation

    /**
     * Executes the command with the arguments passed in via the Constructor. The command will allocate whatever resources
     * it needs to execute and will clean up after its execution completes (successful or not). The only expected side
     * effect may be the modification of the shared State that is passed in via the Constructor.
     *
     * @throws IllegalArgumentException If the arguments passed in via the Constructor are invalid.
     * @throws Exception                If the command failed to execute.
     */
    public abstract void execute() throws Exception;

    /**
     * Creates a new instance of the ServiceConfig class from the shared State passed in via the Constructor.
     */
    protected ServiceConfig getServiceConfig() {
        return getCommandArgs().getState().getConfigBuilder().build().getConfig(ServiceConfig::builder);
    }

    /**
     * Creates a new instance of the CuratorFramework class using configuration from the shared State.
     */
    protected CuratorFramework createZKClient() {
        val serviceConfig = getServiceConfig();
        CuratorFramework zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(serviceConfig.getZkURL())
                .namespace("pravega/" + serviceConfig.getClusterName())
                .retryPolicy(new ExponentialBackoffRetry(serviceConfig.getZkRetrySleepMs(), serviceConfig.getZkRetryCount()))
                .sessionTimeoutMs(serviceConfig.getZkSessionTimeoutMs())
                .build();
        zkClient.start();
        return zkClient;
    }

    protected void output(String messageTemplate, Object... args) {
        System.out.println(String.format(messageTemplate, args));
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

    private <T> T getArg(int index, Function<String, T> converter) {
        String s = null;
        try {
            s = this.commandArgs.getArgs().get(index);
            return converter.apply(s);
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("Unexpected argument '%s' at position %d: %s.", s, index, ex.getMessage()));
        }
    }

    //endregion

    //region Descriptors

    /**
     * Describes an argument.
     */
    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    @Getter
    public static class ArgDescriptor {
        private final String name;
        private final String description;
    }

    /**
     * Describes a Command.
     */
    @Getter
    public static class CommandDescriptor {
        private final String component;
        private final String name;
        private final String description;
        private final ArgDescriptor[] args;
        CommandDescriptor(String component, String name, String description, ArgDescriptor... args) {
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
        private static final HashMap<String, HashMap<String, CommandInfo>> COMMANDS = new HashMap<>();

        static {
            register(ConfigListCommand::descriptor, ConfigListCommand::new);
            register(ConfigSetCommand::descriptor, ConfigSetCommand::new);
            register(BookKeeperCleanupCommand::descriptor, BookKeeperCleanupCommand::new);
            register(BookKeeperListCommand::descriptor, BookKeeperListCommand::new);
            register(BookKeeperDetailsCommand::descriptor, BookKeeperDetailsCommand::new);
        }

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
            HashMap<String, CommandInfo> componentCommands = COMMANDS.getOrDefault(component, null);
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
            HashMap<String, CommandInfo> componentCommands = COMMANDS.getOrDefault(component, null);
            return componentCommands == null ? null : componentCommands.getOrDefault(command, null);
        }

        private static void register(Supplier<CommandDescriptor> descriptor, CommandCreator creator) {
            Command.CommandDescriptor d = descriptor.get();
            HashMap<String, CommandInfo> componentCommands = COMMANDS.getOrDefault(d.getComponent(), null);
            if (componentCommands == null) {
                componentCommands = new HashMap<>();
                COMMANDS.put(d.getComponent(), componentCommands);
            }

            if (componentCommands.putIfAbsent(d.getName(), new CommandInfo(d, creator)) != null) {
                throw new IllegalArgumentException(String.format("A command is already registered for '%s'-'%s'.", d.getComponent(), d.getName()));
            }
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
    //endregion
}
