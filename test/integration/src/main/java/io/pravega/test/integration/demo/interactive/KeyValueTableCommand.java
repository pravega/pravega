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

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

abstract class KeyValueTableCommand extends Command {
    static final String COMPONENT = "kvt";

    KeyValueTableCommand(@NonNull CommandArgs commandArgs) {
        super(commandArgs);
    }

    protected KeyValueTableManager createManager() {
        return KeyValueTableManager.create(URI.create(getConfig().getControllerUri()));
    }

    protected KeyValueTable<String, String> createKVT(ScopedName scopedName) {
        return KeyValueTableFactory
                .withScope(scopedName.getScope(), ClientConfig.builder().controllerURI(getControllerUri()).build())
                .forKeyValueTable(scopedName.getName(), new UTF8StringSerializer(), new UTF8StringSerializer(),
                        KeyValueTableClientConfiguration.builder().build());
    }

    protected String[] toArray(TableEntry<String, String> e, String key) {
        String[] result = new String[3];
        result[0] = key;
        if (e != null) {
            result[1] = e.getKey().getVersion().toString();
            result[2] = e.getValue();
        }
        return result;
    }

    protected String[] toArray(TableKey<String> k) {
        return new String[]{k.getKey(), k.getVersion().toString()};
    }

    protected List<TableEntry<String, String>> toEntries(String[][] rawEntries) {
        return Arrays.stream(rawEntries).map(e -> {
            val key = e[0];
            Version ver = Version.NO_VERSION;
            String value;
            if (e.length == 2) {
                value = e[1];
            } else if (e.length == 3) {
                ver = Version.fromString(e[1]);
                value = e[2];
            } else {
                throw new IllegalArgumentException(String.format("TableEntry must have 2 or 3 elements. Found: %s.", rawEntries.length));
            }

            return TableEntry.versioned(key, ver, value);
        }).collect(Collectors.toList());
    }

    protected <T> KeyFamilyArg<T> getArgsWithKeyFamily(int index, int argsWithKeyFamilyCount, Class<T> argClass) {
        val args = getCommandArgs().getArgs();
        String keyFamily = null;
        T arg;
        if (args.size() == argsWithKeyFamilyCount - 1) {
            arg = getJsonArg(index, argClass);
        } else if (args.size() == argsWithKeyFamilyCount) {
            keyFamily = args.get(index);
            arg = getJsonArg(index + 1, argClass);
        } else {
            throw new IllegalArgumentException("Unexpected number of arguments.");
        }


        return new KeyFamilyArg<>(keyFamily, arg);
    }

    private static Command.CommandDescriptor createDescriptor(String name, String description, Command.ArgDescriptor... args) {
        return new Command.CommandDescriptor(COMPONENT, name, description, args);
    }

    //region Create

    static class Create extends KeyValueTableCommand {
        Create(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            Preconditions.checkArgument(getCommandArgs().getArgs().size() > 0, "At least one KVT name expected.");
            @Cleanup
            val m = createManager();
            val kvtConfig = KeyValueTableConfiguration.builder()
                    .partitionCount(getConfig().getDefaultSegmentCount())
                    .build();
            for (int i = 0; i < getCommandArgs().getArgs().size(); i++) {
                val s = getScopedNameArg(i);
                val success = m.createKeyValueTable(s.getScope(), s.getName(), kvtConfig);
                if (success) {
                    output("Key-Value Table '%s/%s' created successfully.", s.getScope(), s.getName());
                } else {
                    output("Key-Value Table '%s/%s' could not be created.", s.getScope(), s.getName());
                }
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("create", "Creates one or more Key-Value Tables.",
                    new ArgDescriptor("scoped-kvt-names", "Name of the Scoped Key-Value Tables to create."));
        }
    }

    //endregion

    //region Delete

    static class Delete extends KeyValueTableCommand {
        Delete(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            Preconditions.checkArgument(getCommandArgs().getArgs().size() > 0, "At least one KVT name expected.");
            @Cleanup
            val m = createManager();
            for (int i = 0; i < getCommandArgs().getArgs().size(); i++) {
                val s = getScopedNameArg(i);
                val success = m.deleteKeyValueTable(s.getScope(), s.getName());
                if (success) {
                    output("Key-Value Table '%s/%s' deleted successfully.", s.getScope(), s.getName());
                } else {
                    output("Key-Value Table '%s/%s' could not be deleted.", s.getScope(), s.getName());
                }
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("delete", "Deletes one or more Key-Value Tables.",
                    new ArgDescriptor("scoped-kvt-names", "Names of the Scoped Key-Value Tables to delete."));
        }
    }

    //endregion

    //region Get

    static class Get extends KeyValueTableCommand {
        Get(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            val kvtName = getScopedNameArg(0);
            val args = getArgsWithKeyFamily(1, 3, String[].class);
            val keys = args.getArg();
            Preconditions.checkArgument(keys.length > 0, "Expected at least one key.");
            @Cleanup
            val kvt = createKVT(kvtName);
            val result = kvt.getAll(args.getKeyFamily(), Arrays.asList(keys)).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);

            output("Get %s Key(s) from %s[%s]:", keys.length, kvtName, args.getKeyFamily());
            assert keys.length == result.size() : String.format("Bad result length. Expected %s, actual %s", keys.length, result.size());
            for (int i = 0; i < result.size(); i++) {
                String[] output = toArray(result.get(i), keys[i]);
                output("\t%s", GSON.toJson(output));
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("get", "Gets the values of keys from a Key-Value Table.",
                    new ArgDescriptor("scoped-kvt-name", "Name of the Scoped Key-Value Table to get from."),
                    new ArgDescriptor("[key-family]", "(Optional) Key Family to get Keys for."),
                    new ArgDescriptor("keys", "A JSON Array representing the keys to get. Example: \"{[key1, key2, key3]}\"."));
        }
    }

    //endregion

    static class Put extends KeyValueTableCommand {
        Put(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            val kvtName = getScopedNameArg(0);
            val args = getArgsWithKeyFamily(1, 3, String[][].class);
            val rawEntries = args.getArg();

            Preconditions.checkArgument(rawEntries.length > 0, "Expected at least one key.");
            val entries = toEntries(rawEntries);
            @Cleanup
            val kvt = createKVT(kvtName);
            val result = kvt.replaceAll(args.getKeyFamily(), entries).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);

            int conditionalCount = (int) entries.stream().filter(e -> e.getKey().getVersion() != Version.NO_VERSION).count();
            output("Updated %s Key(s) to %s[%s] (Conditional=%s, Unconditional=%s):",
                    entries.size(), kvtName, args.getKeyFamily(), conditionalCount, entries.size() - conditionalCount);
            assert entries.size() == result.size() : String.format("Bad result length. Expected %s, actual %s", entries.size(), result.size());
            for (int i = 0; i < result.size(); i++) {
                String[] output = toArray(TableKey.versioned(entries.get(i).getKey().getKey(), result.get(i)));
                output("\t%s", GSON.toJson(output));
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put", "Updates one or more Keys in a Key-Value table.",
                    new ArgDescriptor("scoped-kvt-name", "Name of the Scoped Key-Value Table to update."),
                    new ArgDescriptor("[key-family]", "(Optional) Key Family to update Entries for."),
                    new ArgDescriptor("entries", "A JSON Array representing the keys to get. Example: \"{[[key1, ver1, val1], [key2, ver2, val2]]}\"."));
        }
    }

    static class Remove extends KeyValueTableCommand {
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

    static class ListKeys extends KeyValueTableCommand {
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

    static class ListEntries extends KeyValueTableCommand {
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

    @Data
    private static class KeyFamilyArg<T> {
        final String keyFamily;
        final T arg;
    }
}
