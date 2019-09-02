/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin.commands;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Properties;

/**
 * Updates the shared AdminCommandState with new config values.
 */
class ConfigSetCommand extends Command {
    /**
     * Creates a new instance of the ConfigSetCommand class.
     *
     * @param args The arguments for the command.
     */
    ConfigSetCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        Properties newValues = new Properties();
        getCommandArgs().getArgs().forEach(s -> {
            String[] items = s.split("=");
            Preconditions.checkArgument(items.length == 2, "Invalid name=value pair: '%s'.", s);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(items[0]) && !Strings.isNullOrEmpty(items[1]),
                    "Invalid name=value pair: '%s'.", s);
            newValues.setProperty(items[0], items[1]);
        });

        Preconditions.checkArgument(newValues.size() > 0, "Expecting at least one argument.");
        getCommandArgs().getState().getConfigBuilder().include(newValues);
    }

    static CommandDescriptor descriptor() {
        return new CommandDescriptor(ConfigCommand.COMPONENT, "set",
                "Sets one or more config values for use during this session.",
                new ArgDescriptor("name=value list", "Space-separated name=value pairs."));
    }
}
