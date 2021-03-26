/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.user.kvs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import io.pravega.cli.user.Command;
import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.utils.Formatter;
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
import io.pravega.common.Exceptions;
import io.pravega.common.util.AsyncIterator;
import lombok.Cleanup;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class KeyValueTableCommand extends Command {
    /**
     * Group name for all {@link KeyValueTableCommand} instances. If changing this, update all Javadoc below (syntax examples).
     */
    static final String COMPONENT = "kvt";

    KeyValueTableCommand(@NonNull CommandArgs commandArgs) {
        super(commandArgs);
    }

    protected KeyValueTableManager createManager() {
        return KeyValueTableManager.create(getClientConfig());
    }

    protected KeyValueTableFactory createKVTFactory(ScopedName scopedName) {
        return KeyValueTableFactory
                .withScope(scopedName.getScope(), getClientConfig());
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

    public static class Create extends KeyValueTableCommand {
        public Create(@NonNull CommandArgs commandArgs) {
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

    public static class Delete extends KeyValueTableCommand {
        public Delete(@NonNull CommandArgs commandArgs) {
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

    //region List

    public static class ListKVTables extends KeyValueTableCommand {
        public ListKVTables(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureArgCount(1);
            @Cleanup
            val m = createManager();
            val kvtIterator = m.listKeyValueTables(getArg(0));
            if (!kvtIterator.hasNext()) {
                output("Scope '%s' does not have any Key-Value Tables.", getArg(0));
            }

            Streams.stream(kvtIterator)
                    .sorted(Comparator.comparing(KeyValueTableInfo::getScopedName))
                    .forEach(kvt -> output("\t%s", kvt.getScopedName()));
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("list", "Lists all Key-Value Tables in a Scope.")
                    .withArg("scope-name", "Name of Scope to list Key-Value Tables from.")
                    .build();
        }
    }

    //endregion

    //region DataCommand

    private static abstract class DataCommand extends KeyValueTableCommand {
        private static final int[] TABLE_FORMAT_COLUMN_LENGTHS = new int[]{25, 12, 40};
        private final Formatter formatter;

        DataCommand(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
            this.formatter = getConfig().isPrettyPrint()
                    ? new Formatter.TableFormatter(Arrays.copyOf(getTableFormatColumnLengths(), getResultColumnCount()))
                    : new Formatter.JsonFormatter();
        }

        protected void outputResultHeader(String... columnNames) {
            outputResult(columnNames);
            output(this.formatter.separator());
        }

        protected void outputResult(String... resultColumns) {
            this.formatter.format(resultColumns).forEach(this::output);
        }

        protected int getResultColumnCount() {
            return 0;
        }

        protected int[] getTableFormatColumnLengths() {
            return TABLE_FORMAT_COLUMN_LENGTHS;
        }

        protected abstract void ensurePreconditions();

        protected abstract void executeInternal(ScopedName kvtName, KeyValueTable<String, String> kvt) throws Exception;

        public void execute() throws Exception {
            ensurePreconditions();
            val kvtName = getScopedNameArg(0);
            @Cleanup
            val factory = createKVTFactory(kvtName);
            @Cleanup
            val kvt = createKVT(kvtName, factory);
            try {
                executeInternal(kvtName, kvt);
            } catch (Throwable ex) {
                val innerEx = Exceptions.unwrap(ex);
                if (innerEx instanceof ConditionalTableUpdateException) {
                    output("%s: %s", innerEx.getClass().getSimpleName(), innerEx.getMessage());
                } else {
                    throw ex;
                }
            }
        }
    }

    //endregion

    //region Get

    public static class Get extends DataCommand {
        public Get(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected int getResultColumnCount() {
            return 3;
        }

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(2, 3);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable<String, String> kvt) throws Exception {
            val args = getArgsWithKeyFamily(1, 3, String[].class);
            val keys = args.getArg();
            Preconditions.checkArgument(keys.length > 0, "Expected at least one key.");
            val result = kvt.getAll(args.getKeyFamily(), Arrays.asList(keys)).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);

            output("Get %s Key(s) from %s[%s]:", keys.length, kvtName, args.getKeyFamily());
            assert keys.length == result.size() : String.format("Bad result length. Expected %s, actual %s", keys.length, result.size());
            outputResultHeader("Key", "Version", "Value");
            int count = 0;
            for (val e : result) {
                if (e != null) {
                    outputResult(toArray(e));
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

    public static class Put extends DataCommand {
        public Put(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(3, 4);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable<String, String> kvt) throws Exception {
            String keyFamily = null;
            String key;
            String value;
            if (getCommandArgs().getArgs().size() == 4) {
                keyFamily = getArg(1);
                key = getArg(2);
                value = getArg(3);
            } else {
                key = getArg(1);
                value = getArg(2);
            }

            val version = kvt.put(keyFamily, key, value).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            output("Key '%s' updated successfully. New version: '%s'.", key, version);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put", "Unconditionally inserts or updates a Table Entry.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to update.")
                    .withArg("[key-family]", "(Optional) Key Family to update the Table Entry for.")
                    .withArg("key", "The key.")
                    .withArg("value", "The value.")
                    .withSyntaxExample("scope1/kvt1 key1 value1", "Sets 'key1:=value1' in 'scope1/kvt1'.")
                    .withSyntaxExample("scope1/kvt1 key-family-1 key1 value1", "Sets 'key1:=value1' in 'scope1/kvt1' with key family 'key-family-1'.")
                    .build();
        }
    }

    public static class PutIf extends DataCommand {
        public PutIf(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(4, 5);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable<String, String> kvt) throws Exception {
            String keyFamily = null;
            String key;
            Version version;
            String value;
            if (getCommandArgs().getArgs().size() == 5) {
                keyFamily = getArg(1);
                key = getArg(2);
                version = Version.fromString(getArg(3));
                value = getArg(4);
            } else {
                key = getArg(1);
                version = Version.fromString(getArg(2));
                value = getArg(3);
            }

            val newVersion = kvt.replace(keyFamily, key, value, version).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            output("Key '%s' updated successfully. New version: '%s'.", key, newVersion);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put-if", "Conditionally inserts or updates a Table Entry.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to update.")
                    .withArg("[key-family]", "(Optional) Key Family to update the Table entry for.")
                    .withArg("key", "The key.")
                    .withArg("version", "The expected Key Version.")
                    .withArg("value", "The value.")
                    .withSyntaxExample("scope1/kvt1 key1 s1:1 value1", "Inserts 'key1:=value1' in 'scope1/kvt1', " +
                            "only if the current version of 'key1' is 's1:1'.")
                    .withSyntaxExample("scope1/kvt1 key-family-1 key1 s1:1 value1", "Inserts 'key1:=value1' in 'scope1/kvt1' " +
                            "with key family 'key-family-1', only if the current version of 'key1' is 's1:1'.")
                    .build();
        }
    }

    public static class PutIfAbsent extends DataCommand {
        public PutIfAbsent(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(3, 4);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable<String, String> kvt) throws Exception {
            String keyFamily = null;
            String key;
            String value;
            if (getCommandArgs().getArgs().size() == 4) {
                keyFamily = getArg(1);
                key = getArg(2);
                value = getArg(3);
            } else {
                key = getArg(1);
                value = getArg(2);
            }

            val version = kvt.putIfAbsent(keyFamily, key, value).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            output("Key '%s' inserted successfully. New version: '%s'.", key, version);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put-if-absent", "Inserts a Table Entry, only if its Key is not already present.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to update.")
                    .withArg("[key-family]", "(Optional) Key Family to insert the Table entry for.")
                    .withArg("key", "The key.")
                    .withArg("value", "The value.")
                    .withSyntaxExample("scope1/kvt1 key1 value1", "Inserts 'key1:=value1' in 'scope1/kvt1', only if not already present.")
                    .withSyntaxExample("scope1/kvt1 key-family-1 key1 value1", "Inserts 'key1:=value1' in 'scope1/kvt1' " +
                            "with key family 'key-family-1', only if not already present.")
                    .build();
        }
    }

    public static class PutAll extends DataCommand {
        public PutAll(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected int getResultColumnCount() {
            return 2;
        }

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(2, 3);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable<String, String> kvt) throws Exception {
            val args = getArgsWithKeyFamily(1, 3, String[][].class);
            val entries = toEntries(args.getArg());
            Preconditions.checkArgument(entries.size() > 0, "Expected at least one Table Entry.");
            Preconditions.checkArgument(entries.size() == 1 || args.getKeyFamily() != null, "Expected a Key Family if updating more than one entry.");

            val result = kvt.replaceAll(args.getKeyFamily(), entries).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            int conditionalCount = (int) entries.stream().filter(e -> e.getKey().getVersion() != Version.NO_VERSION).count();
            output("Updated %s Key(s) to %s[%s] (Conditional=%s, Unconditional=%s):",
                    entries.size(), kvtName, args.getKeyFamily(), conditionalCount, entries.size() - conditionalCount);
            assert entries.size() == result.size() : String.format("Bad result length. Expected %s, actual %s", entries.size(), result.size());
            outputResultHeader("Key", "Version");
            for (int i = 0; i < result.size(); i++) {
                String[] output = toArray(TableKey.versioned(entries.get(i).getKey().getKey(), result.get(i)));
                outputResult(output);
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put-all", "Updates one or more Keys in a Key-Value table.")
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

    public static class PutRange extends DataCommand {
        public PutRange(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(4);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable<String, String> kvt) throws Exception {
            val keyFamily = getArg(1);
            val rangeStart = getIntArg(2);
            val rangeEnd = getIntArg(3);

            Preconditions.checkArgument(rangeStart <= rangeEnd, "RangeStart (%s) must be less than or equal to RangeEnd (%s).", rangeStart, rangeEnd);
            val valuePrefix = Instant.now().toString();
            val newValues = IntStream.rangeClosed(rangeStart, rangeEnd).boxed()
                    .map(i -> TableEntry.unversioned(i.toString(), String.format("%s_%s", valuePrefix, i)))
                    .collect(Collectors.toList());

            kvt.replaceAll(keyFamily, newValues).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            output("Bulk-updated %s Key(s) to %s[%s].", newValues.size(), kvtName, keyFamily);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put-range", "Bulk-updates a set of generated keys between two numbers.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to update.")
                    .withArg("key-family", "Key Family to update Entries for (not optional).")
                    .withArg("range-start", "The lower bound of the range.")
                    .withArg("range-end", "The upper bound of the range..")
                    .withSyntaxExample("scope1/kvt1 key-family-1 1 100", "Updates keys '1' to '100' in 'scope1/kvt1' with key family 'key-family-1'.")
                    .build();
        }
    }

    //endregion

    //region Remove

    public static class Remove extends DataCommand {
        public Remove(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(2, 3);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable<String, String> kvt) throws Exception {
            val args = getArgsWithKeyFamily(1, 3, String[][].class);
            val keys = toKeys(args.getArg());
            Preconditions.checkArgument(keys.size() > 0, "Expected at least one Table Key.");
            Preconditions.checkArgument(keys.size() == 1 || args.getKeyFamily() != null, "Expected a Key Family if removing more than one entry.");
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

    private static abstract class ListCommand<T> extends DataCommand {
        private static final String[] RESULT_HEADER = new String[]{"Key", "Version", "Value"};
        ListCommand(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        protected abstract AsyncIterator<IteratorItem<T>> getIterator(KeyValueTable<String, String> kvt, String keyFamily);

        protected abstract String[] convertToArray(T item);

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(2);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable<String, String> kvt) throws Exception {
            val keyFamily = getArg(1);
            val iterator = getIterator(kvt, keyFamily);
            outputResultHeader(Arrays.copyOf(RESULT_HEADER, getResultColumnCount()));
            int count = 0;
            while (count < getConfig().getMaxListItems()) {
                val batch = iterator.getNext().get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
                if (batch == null) {
                    break; // We're done.
                }

                int maxCount = Math.min(getConfig().getMaxListItems() - count, batch.getItems().size());
                for (int i = 0; i < maxCount; i++) {
                    outputResult(convertToArray(batch.getItems().get(i)));
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

    public static class ListKeys extends ListCommand<TableKey<String>> {
        public ListKeys(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected int[] getTableFormatColumnLengths() {
            return new int[]{Arrays.stream(super.getTableFormatColumnLengths()).sum()};
        }

        @Override
        protected int getResultColumnCount() {
            return 1;
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

    public static class ListEntries extends ListCommand<TableEntry<String, String>> {
        public ListEntries(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected int getResultColumnCount() {
            return 3;
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
