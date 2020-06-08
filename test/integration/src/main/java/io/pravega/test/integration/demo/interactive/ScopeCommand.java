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

import lombok.NonNull;

abstract class ScopeCommand extends Command {
    static final String COMPONENT = "scope";

    ScopeCommand(@NonNull CommandArgs commandArgs) {
        super(commandArgs);
    }

    private static Command.CommandDescriptor createDescriptor(String name, String description, Command.ArgDescriptor... args) {
        return new Command.CommandDescriptor(COMPONENT, name, description, args);
    }

    static class Create extends StreamCommand {
        Create(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("Scope.Create %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("create", "Creates a new Scope.",
                    new ArgDescriptor("scope-name", "Name of the Scope to create."));
        }
    }

    static class Delete extends StreamCommand {
        Delete(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("Scope.Delete %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("delete", "Creates a new Stream.",
                    new ArgDescriptor("scope-name", "Name of the Scope to delete."));
        }
    }
}
