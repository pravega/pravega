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
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
import io.pravega.common.util.AsyncIterator;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

abstract class KeyValueTableCommand extends Command {
    /**
     * Group name for all {@link KeyValueTableCommand} instances. If changing this, update all Javadoc below (syntax examples).
     */
    static final String COMPONENT = "kvt";

    KeyValueTableCommand(@NonNull CommandArgs commandArgs) {
        super(commandArgs);
    }

    protected KeyValueTableManager createManager() {
        return KeyValueTableManager.create(URI.create(getConfig().getControllerUri()));
    }

    protected KeyValueTableFactory createKVTFactory(ScopedName scopedName) {
        return KeyValueTableFactory
                .withScope(scopedName.getScope(), ClientConfig.builder().controllerURI(getControllerUri()).build());
    }

    protected KeyValueTable<String, String> createKVT(ScopedName scopedName, KeyValueTableFactory factory) {
        return factory.forKeyValueTable(scopedName.getName(), new UTF8StringSerializer(), new UTF8StringSerializer(),
                KeyValueTableClientConfiguration.builder().build());
    }

    protected String[] toArray(TableEntry<String, String> e) {
        return new String[]{e.getKey().getKey(), e.getKey().getVersion().toString(), e.getValue()};
    }

    protected String[] toArray(TableKey<String> k) {
        return new String[]{k.getKey(), k.getVersion().toString()};
    }

    protected String toJson(String[] keys) {
        if (keys.length == 1) {
            return GSON.toJson(keys[0]);
        } else {
            return GSON.toJson(keys);
        }
    }

    protected List<TableEntry<String, String>> toEntries(String[][] rawEntries) {
        return Arrays.stream(rawEntries).map(e -> {
            Preconditions.checkArgument(e.length == 2 || e.length == 1,
                    "TableEntry must have 2 or 3 elements ('[key, value]' or '[key, version, value]'). Found  %s.", e.length);
            val key = e[0];
            Version ver = Version.NO_VERSION;
            String value;
            if (e.length == 2) {
                value = e[1];
            } else {
                ver = Version.fromString(e[1]);
                value = e[2];
            }

            return TableEntry.versioned(key, ver, value);
        }).collect(Collectors.toList());
    }

    protected List<TableKey<String>> toKeys(String[][] rawKeys) {
        return Arrays.stream(rawKeys).map(k -> {
            Preconditions.checkArgument(k.length == 2 || k.length == 1,
                    "TableKey must have 1 or 2 elements ('[key]' or '[key, version]'). Found: %s.", k.length);
            val key = k[0];
            Version ver = Version.NO_VERSION;
            if (k.length == 2) {
                ver = Version.fromString(k[1]);
            }

            return TableKey.versioned(key, ver);
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

    private static Command.CommandDescriptor.CommandDescriptorBuilder createDescriptor(String name, String description) {
        return Command.CommandDescriptor.builder()
                .component(COMPONENT)
                .name(name)
                .description(description);
    }

    //region Create

    static class Create extends KeyValueTableCommand {
        Create(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureMinArgCount(1);
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
            return createDescriptor("create", "Creates one or more Key-Value Tables.")
                    .withArg("scoped-kvt-names", "Name of the Scoped Key-Value Tables to create.")
                    .withSyntaxExample("scope1/kvt1 scope1/kvt2 scope2/kvt3", "Creates kvt1 and kvt2 in scope1 and kvt3 in scope2.")
                    .build();
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
            ensureMinArgCount(1);
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
            return createDescriptor("delete", "Deletes one or more Key-Value Tables.")
                    .withArg("scoped-kvt-names", "Names of the Scoped Key-Value Tables to delete.")
                    .withSyntaxExample("scope1/kvt1 scope1/kvt2 scope3/kvt3", "Deletes kvt1 and kvt2 from scope1 and kvt3 from scope3.")
                    .build();
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
            ensureArgCount(2, 3);
            val kvtName = getScopedNameArg(0);
            val args = getArgsWithKeyFamily(1, 3, String[].class);
            val keys = args.getArg();
            Preconditions.checkArgument(keys.length > 0, "Expected at least one key.");
            @Cleanup
            val factory = createKVTFactory(kvtName);
            @Cleanup
            val kvt = createKVT(kvtName, factory);
            val result = kvt.getAll(args.getKeyFamily(), Arrays.asList(keys)).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);

            output("Get %s Key(s) from %s[%s]:", keys.length, kvtName, args.getKeyFamily());
            assert keys.length == result.size() : String.format("Bad result length. Expected %s, actual %s", keys.length, result.size());
            int count = 0;
            for (val e : result) {
                if (e != null) {
                    output("\t%s", toJson(toArray(e)));
                    if (++count >= getConfig().getMaxListItems()) {
                        output("Only showing first %s items (of %s). Change this using '%s' config value.",
                                getConfig().getMaxListItems(), result.size(), InteractiveConfig.MAX_LIST_ITEMS);
                        break;
                    }
                }
            }

            int notFound = (int) result.stream().filter(Objects::isNull).count();
            if (notFound > 0) {
                output("\t%s key(s) could not be found.", notFound);
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("get", "Gets the values of keys from a Key-Value Table.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to get from.")
                    .withArg("[key-family]", "(Optional) Key Family to get Keys for.")
                    .withArg("keys", "A JSON Array representing the keys to get. Example: \"{[key1, key2, key3]}\".")
                    .withSyntaxExample("scope1/kvt1 {[key1, \"key2:escape\"]}", "Gets 'key1' and 'key2:escape' from scope1/kvt1 (no Key Family).")
                    .withSyntaxExample("scope1/kvt1 key-family-1 {[key1]}", "Gets 'key1' belonging to 'key-family-1' from scope1/kvt1.")
                    .build();
        }
    }

    //endregion

    //region Put

    static class Put extends KeyValueTableCommand {
        Put(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            ensureArgCount(2, 3);
            val kvtName = getScopedNameArg(0);
            val args = getArgsWithKeyFamily(1, 3, String[][].class);
            val entries = toEntries(args.getArg());
            Preconditions.checkArgument(entries.size() > 0, "Expected at least one Table Entry.");
            Preconditions.checkArgument(entries.size() == 1 || args.getKeyFamily() != null, "Expected a Key Family if updating more than one entry.");
            @Cleanup
            val factory = createKVTFactory(kvtName);
            @Cleanup
            val kvt = createKVT(kvtName, factory);
            val result = kvt.replaceAll(args.getKeyFamily(), entries).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);

            int conditionalCount = (int) entries.stream().filter(e -> e.getKey().getVersion() != Version.NO_VERSION).count();
            output("Updated %s Key(s) to %s[%s] (Conditional=%s, Unconditional=%s):",
                    entries.size(), kvtName, args.getKeyFamily(), conditionalCount, entries.size() - conditionalCount);
            assert entries.size() == result.size() : String.format("Bad result length. Expected %s, actual %s", entries.size(), result.size());
            for (int i = 0; i < result.size(); i++) {
                String[] output = toArray(TableKey.versioned(entries.get(i).getKey().getKey(), result.get(i)));
                output("\t%s", toJson(output));
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put", "Updates one or more Keys in a Key-Value table.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to update.")
                    .withArg("[key-family]", "(Optional) Key Family to update Entries for.")
                    .withArg("entries", "A JSON Array representing the keys to get.")
                    .withSyntaxExample("scope1/kvt1 {[[key1, value1]]}", "Unconditionally updates 'key1' to 'value1' in 'scope1/kvt1'.")
                    .withSyntaxExample("scope1/kvt1 {[[key1, \"seg1:ver1\", value1]]}",
                            "Conditionally updates 'key1' to 'value1' in 'scope1/kvt1' using 'seg1:ver1' as condition version.")
                    .withSyntaxExample("scope1/kvt1 key-family-1 {[[key1, \"seg1:ver\", value1], [key2, val2]]}",
                            "Conditionally updates 'key1' to 'value1' and 'key2' to 'value2' in 'scope1/kvt1' with key family 'key-family-1' " +
                                    "conditioned on `key1` having version 'seg1:ver1'.")
                    .build();
        }
    }

    //endregion

    //region Remove

    static class Remove extends KeyValueTableCommand {
        Remove(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            ensureArgCount(2, 3);
            val kvtName = getScopedNameArg(0);
            val args = getArgsWithKeyFamily(1, 3, String[][].class);
            val keys = toKeys(args.getArg());
            Preconditions.checkArgument(keys.size() > 0, "Expected at least one Table Key.");
            Preconditions.checkArgument(keys.size() == 1 || args.getKeyFamily() != null, "Expected a Key Family if removing more than one entry.");
            @Cleanup
            val factory = createKVTFactory(kvtName);
            @Cleanup
            val kvt = createKVT(kvtName, factory);
            kvt.removeAll(args.getKeyFamily(), keys).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);

            int conditionalCount = (int) keys.stream().filter(e -> e.getVersion() != Version.NO_VERSION).count();
            output("Removed %s Key(s) from %s[%s] (Conditional=%s, Unconditional=%s).",
                    keys.size(), kvtName, args.getKeyFamily(), conditionalCount, keys.size() - conditionalCount);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("remove", "Removes one or more Keys from a Key-Value table.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to remove from.")
                    .withArg("[key-family]", "(Optional) Key Family to remove Keys for.")
                    .withArg("entries", "A JSON Array representing the keys to remove.")
                    .withSyntaxExample("scope1/kvt1 {[[key1]]}", "Unconditionally removes 'key1' from 'scope1/kvt1'.")
                    .withSyntaxExample("scope1/kvt1 {[[key1, \"s1:ver1\"]]}",
                            "Conditionally removes 'key1' from 'scope1/kvt1' using 'seg1:ver1' as condition version.")
                    .withSyntaxExample("scope1/kvt1 key-family-1 {[[key1, \"seg1:ver1\"], [key2]]}",
                            "Conditionally removes 'key1' and 'key2' from 'scope1/kvt1' with key family 'key-family-1' " +
                                    "conditioned on `key1` having version 'seg1:ver1'.")
                    .build();
        }
    }

    //endregion

    //region Key/Entry iterators

    private static abstract class ListCommand<T> extends KeyValueTableCommand {
        ListCommand(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        protected abstract AsyncIterator<IteratorItem<T>> getIterator(KeyValueTable<String, String> kvt, String keyFamily);

        protected abstract String[] convertToArray(T item);

        @Override
        public void execute() throws Exception {
            ensureArgCount(2);
            val kvtName = getScopedNameArg(0);
            val keyFamily = getArg(1);
            @Cleanup
            val factory = createKVTFactory(kvtName);
            @Cleanup
            val kvt = createKVT(kvtName, factory);
            val iterator = getIterator(kvt, keyFamily);
            int count = 0;
            while (count < getConfig().getMaxListItems()) {
                val batch = iterator.getNext().get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
                if (batch == null) {
                    break; // We're done.
                }

                int maxCount = Math.min(getConfig().getMaxListItems() - count, batch.getItems().size());
                for (int i = 0; i < maxCount; i++) {
                    output("\t%s", toJson(convertToArray(batch.getItems().get(i))));
                }

                count += maxCount;
                if (maxCount < batch.getItems().size()) {
                    output("Only showing first %s items. Change this using '%s' config value.",
                            maxCount, InteractiveConfig.MAX_LIST_ITEMS);
                }
            }

            output("Total: %s item(s).", count);
        }
    }

    static class ListKeys extends ListCommand<TableKey<String>> {
        ListKeys(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected AsyncIterator<IteratorItem<TableKey<String>>> getIterator(KeyValueTable<String, String> kvt, String keyFamily) {
            return kvt.keyIterator(keyFamily, 100, null);
        }

        @Override
        protected String[] convertToArray(TableKey<String> item) {
            return new String[]{item.getKey()};
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("list-keys", "Lists all keys in a Key-Value Table.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to list keys from.")
                    .withArg("key-family", "Name of the Key Family to list keys from.")
                    .build();
        }
    }

    static class ListEntries extends ListCommand<TableEntry<String, String>> {
        ListEntries(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected AsyncIterator<IteratorItem<TableEntry<String, String>>> getIterator(KeyValueTable<String, String> kvt, String keyFamily) {
            return kvt.entryIterator(keyFamily, 100, null);
        }

        @Override
        protected String[] convertToArray(TableEntry<String, String> item) {
            return toArray(item);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("list-entries", "Lists all entries in a Key-Value Table.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to list entries from.")
                    .withArg("key-family", "Name of the Key Family to list entries from.")
                    .build();
        }
    }

    //endregion

    //region Helper classes

    @Data
    private static class KeyFamilyArg<T> {
        private final String keyFamily;
        private final T arg;
    }

    //endregion
}
