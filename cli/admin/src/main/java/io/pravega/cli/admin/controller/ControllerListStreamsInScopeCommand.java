/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.controller;

import io.pravega.cli.admin.CommandArgs;
import lombok.Cleanup;
import lombok.val;

/**
 * Lists all the Streams in a given Scope.
 */
public class ControllerListStreamsInScopeCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerListStreamsInScopeCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(1);
        @Cleanup
        val context = createContext();
        String scope = getCommandArgs().getArgs().get(0);
        prettyJSONOutput(executeRESTCall(context, "/v1/scopes/" + scope + "/streams"));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-streams", "Lists all the existing Streams in a given Scope.",
                new ArgDescriptor("scope-name", "Name of the Scope to get Streams for."));
    }
}
