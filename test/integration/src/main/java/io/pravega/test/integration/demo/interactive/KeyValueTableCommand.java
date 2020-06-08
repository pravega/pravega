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

abstract class KeyValueTableCommand extends Command {
    static final String COMPONENT = "kvt";

    KeyValueTableCommand(@NonNull CommandArgs commandArgs) {
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
            output("KVT.Create %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("create", "Creates a new Key-Value Table.",
                    new ArgDescriptor("scoped-kvt-name", "Name of the Scoped Key-Value Table to create."));
        }
    }

    static class Delete extends StreamCommand {
        Delete(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("KVT.Delete %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("delete", "Creates a new Key-Value Table.",
                    new ArgDescriptor("scoped-kvt-name", "Name of the Scoped Key-Value Table to delete."));
        }
    }

    static class Get extends StreamCommand {
        Get(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("KVT.Get %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("get", "Gets the values of keys from a Key-Value Table.",
                    new ArgDescriptor("scoped-kvt-name", "Name of the Scoped Key-Value Table to get from."));
        }
    }

    static class Put extends StreamCommand {
        Put(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("KVT.Put %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put", "Updates one or more Keys in a Key-Value table.",
                    new ArgDescriptor("scoped-kvt-name", "Name of the Scoped Key-Value Table to update."));
        }
    }

    static class Remove extends StreamCommand {
        Remove(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("KVT.Remove %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("remove", "Removes one or more Keys from a Key-Value table.",
                    new ArgDescriptor("scoped-kvt-name", "Name of the Scoped Key-Value Table to remove from."));
        }
    }

    static class ListKeys extends StreamCommand {
        ListKeys(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("KVT.ListKeys %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("list-keys", "Lists all keys in a Key-Value Table.",
                    new ArgDescriptor("scoped-kvt-name", "Name of the Scoped Key-Value Table to list keys from."),
                    new ArgDescriptor("key-family", "Name of the Key Family to list keys from."));
        }
    }

    static class ListEntries extends StreamCommand {
        ListEntries(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            output("KVT.ListEntries %s", getCommandArgs());
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("list-entries", "Lists all entries in a Key-Value Table.",
                    new ArgDescriptor("scoped-kvt-name", "Name of the Scoped Key-Value Table to list entries from."),
                    new ArgDescriptor("key-family", "Name of the Key Family to list entries from."));
        }
    }
}
