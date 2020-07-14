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

import io.pravega.client.admin.StreamManager;
import java.net.URI;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.val;

abstract class ScopeCommand extends Command {
    static final String COMPONENT = "scope";

    ScopeCommand(@NonNull CommandArgs commandArgs) {
        super(commandArgs);
    }

    private static Command.CommandDescriptor.CommandDescriptorBuilder createDescriptor(String name, String description) {
        return Command.CommandDescriptor.builder()
                .component(COMPONENT)
                .name(name)
                .description(description);
    }

    static class Create extends StreamCommand {
        Create(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureMinArgCount(1);
            @Cleanup
            val sm = StreamManager.create(URI.create(getConfig().getControllerUri()));
            for (val scopeName : getCommandArgs().getArgs()) {
                val success = sm.createScope(scopeName);
                if (success) {
                    output("Scope '%s' created successfully.", scopeName);
                } else {
                    output("Scope '%s' could not be created.", scopeName);
                }
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("create", "Creates one or more Scopes.")
                    .withArg("scope-names", "Name of the Scopes to create.")
                    .build();
        }
    }

    static class Delete extends StreamCommand {
        Delete(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureMinArgCount(1);
            @Cleanup
            val sm = StreamManager.create(URI.create(getConfig().getControllerUri()));
            for (val scopeName : getCommandArgs().getArgs()) {
                val success = sm.deleteScope(scopeName);
                if (success) {
                    output("Scope '%s' deleted successfully.", scopeName);
                } else {
                    output("Scope '%s' could not be deleted.", scopeName);
                }
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("delete", "Deletes one or more Scopes.")
                    .withArg("scope-names", "Names of the Scopes to delete.")
                    .build();
        }
    }
}
