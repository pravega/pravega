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

abstract class StreamCommand extends Command {
    static final String COMPONENT = "stream";

    StreamCommand(@NonNull CommandArgs commandArgs) {
        super(commandArgs);
    }

    private static CommandDescriptor createDescriptor(String name, String description, ArgDescriptor... args) {
        return new CommandDescriptor(COMPONENT, name, description, args);
    }

    static class Create extends StreamCommand {
        Create(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("Stream.Create %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("create", "Creates a new Stream.",
                    new ArgDescriptor("scoped-stream-name", "Name of the Scoped Stream to create."));
        }
    }

    static class Delete extends StreamCommand {
        Delete(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("Stream.Delete %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("delete", "Creates a new Stream.",
                    new ArgDescriptor("scoped-stream-name", "Name of the Scoped Stream to delete."));
        }
    }
}
