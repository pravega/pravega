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
 * Gets all the Scopes from the system.
 */
public class ControllerListScopesCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerListScopesCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(0);
        @Cleanup
        val context = createContext();
        prettyJSONOutput(executeRESTCall(context, "/v1/scopes/"));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-scopes", "Lists all the existing scopes in the system.");
    }
}
