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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Data;

/**
 * Defines all Commands used by the Admin tool, and provides means of accessing them.
 */
final class Commands {
    private static final HashMap<String, HashMap<String, CommandInfo>> COMMANDS = new HashMap<>();

    static {
        register(SampleCommand::getDescriptor, SampleCommand::new);
    }

    /**
     * Gets a Collection of CommandDescriptors for all registered commands.
     *
     * @return A new Collection.
     */
    static Collection<CommandBase.CommandDescriptor> getDescriptors() {
        ArrayList<CommandBase.CommandDescriptor> result = new ArrayList<>();
        COMMANDS.values().forEach(g -> g.values().forEach(c -> result.add(c.getDescriptor())));
        return result;
    }

    /**
     * Gets a Collection of CommandDescriptors for all registered commands for the given component.
     *
     * @param component The component to query.
     * @return A new Collection.
     */
    static Collection<CommandBase.CommandDescriptor> getDescriptors(String component) {
        HashMap<String, CommandInfo> componentCommands = COMMANDS.getOrDefault(component, null);
        return componentCommands == null
                ? Collections.emptyList()
                : componentCommands.values().stream().map(CommandInfo::getDescriptor).collect(Collectors.toList());
    }

    /**
     * Gets a CommandDescriptor for the given Parser.Command.
     *
     * @param cmd The Parser.Command to get a descriptor for.
     * @return The CommandDescriptor for the given argument, or null if no such command is registered.
     */
    static CommandBase.CommandDescriptor getDescriptor(Parser.Command cmd) {
        CommandInfo ci = getCommand(cmd);
        return ci == null ? null : ci.getDescriptor();
    }

    /**
     * Gets a new instance of a CommandBase for the given Parser.Command.
     *
     * @param cmd The Parser.Command to get and instantiate the command for.
     * @return A new instance of a Command base for the given Parser.Command, already initialized with the command's args.
     */
    static CommandBase get(Parser.Command cmd) {
        CommandInfo ci = getCommand(cmd);
        return ci == null ? null : ci.getCreator().apply(cmd.getArgs());
    }

    private static CommandInfo getCommand(Parser.Command cmd) {
        HashMap<String, CommandInfo> componentCommands = COMMANDS.getOrDefault(cmd.getComponent(), null);
        return componentCommands == null ? null : componentCommands.getOrDefault(cmd.getCommand(), null);
    }

    private static void register(CommandDescriptor descriptor, CommandCreator creator) {
        CommandBase.CommandDescriptor d = descriptor.get();
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
        private final CommandBase.CommandDescriptor descriptor;
        private final CommandCreator creator;
    }

    @FunctionalInterface
    private interface CommandCreator extends Function<List<String>, CommandBase> {
    }

    @FunctionalInterface
    private interface CommandDescriptor extends Supplier<CommandBase.CommandDescriptor> {
    }
}
