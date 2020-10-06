/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.config;

import com.google.common.base.Preconditions;
import io.pravega.cli.admin.CommandArgs;

/**
 * Lists the contents of the shared Configuration.
 */
public class ConfigListCommand extends ConfigCommand {
    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ConfigListCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        Preconditions.checkArgument(getCommandArgs().getArgs().size() == 0, "Not expecting any arguments.");
        getCommandArgs().getState().getConfigBuilder().build().forEach((name, value) -> output("\t%s=%s", name, value));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list", "Lists all configuration set during this session.");
    }
}
