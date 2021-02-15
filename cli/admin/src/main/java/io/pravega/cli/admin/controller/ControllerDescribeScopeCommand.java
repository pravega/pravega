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
 * Provide available information of a given Scope.
 */
public class ControllerDescribeScopeCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerDescribeScopeCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(1);
        // Describe a the selected scope via REST API.
        @Cleanup
        val context = createContext();
        // Print the response sent by the Controller.
        prettyJSONOutput(executeRESTCall(context, "/v1/scopes/" + getCommandArgs().getArgs().get(0)));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "describe-scope", "Get the details of a given Scope.",
                new ArgDescriptor("scope-name", "Name of the Scope to get details for."));
    }
}
