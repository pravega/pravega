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
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.cli.user.utils.Formatter;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.Insert;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.KeyValueTableIterator;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.TableModification;
import io.pravega.client.tables.Version;
import io.pravega.common.Exceptions;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

public abstract class KeyValueTableCommand extends Command {
    /**
     * Group name for all {@link KeyValueTableCommand} instances. If changing this, update all Javadoc below (syntax examples).
     */
    static final String COMPONENT = "kvt";
    static final UTF8StringSerializer SERIALIZER = new UTF8StringSerializer();

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

    protected KeyValueTable createKVT(ScopedName scopedName, KeyValueTableFactory factory) {
        return factory.forKeyValueTable(scopedName.getName(), KeyValueTableClientConfiguration.builder().build());
    }

    protected String[] toArray(TableEntry e) {
        KeyInfo k = new KeyInfo(SERIALIZER.deserialize(e.getKey().getPrimaryKey().duplicate()),
                e.getKey().getSecondaryKey() == null ? "[null]" : SERIALIZER.deserialize(e.getKey().getSecondaryKey().duplicate()));
        return new String[]{
                k.toString(),
                e.getVersion().toString(),
                SERIALIZER.deserialize(e.getValue().duplicate())};
    }

    protected String[] toArray(TableKey k) {
        return new String[]{
                SERIALIZER.deserialize(k.getPrimaryKey().duplicate()),
                k.getSecondaryKey() == null ? "[null]" : SERIALIZER.deserialize(k.getSecondaryKey().duplicate())};
    }

    protected List<TableModification> toUpdates(String[][] rawEntries) {
        return Arrays.stream(rawEntries).map(e -> {
            Preconditions.checkArgument(e.length == 2 || e.length == 1,
                    "TableEntry must have 2 or 3 elements ('[key, value]' or '[key, version, value]'). Found  %s.", e.length);
            val key = KeyInfo.parse(e[0]);
            Version ver = Version.NO_VERSION;
            String value;
            if (e.length == 2) {
                value = e[1];
            } else {
                ver = Version.fromString(e[1]);
                value = e[2];
            }

            return new io.pravega.client.tables.Put(key.toTableKey(), SERIALIZER.serialize(value), ver);
        }).collect(Collectors.toList());
    }

    protected List<TableModification> toRemovals(String[][] rawKeys) {
        return Arrays.stream(rawKeys).map(k -> {
            val key = KeyInfo.parse(k[0]);
            Preconditions.checkArgument(k.length == 1, "TableKey must have 1 element. Found: %s.", k.length);
            return new io.pravega.client.tables.Remove(key.toTableKey());
        }).collect(Collectors.toList());
    }

    private static Command.CommandDescriptor.CommandDescriptorBuilder createDescriptor(String name, String description) {
        return Command.CommandDescriptor.builder()
                .component(COMPONENT)
                .name(name)
                .description(description);
    }

    @Data
    protected static class KeyInfo {
        private final String primaryKey;
        private final String secondaryKey;

        static KeyInfo parse(String key) {
            int pos = key.indexOf(':');
            if (pos < 0) {
                return new KeyInfo(key, null);
            } else if (pos > 0 && pos < key.length() - 1) {
                return new KeyInfo(key.substring(0, pos), key.substring(pos + 1));
            }
            throw new IllegalArgumentException("Invalid key serialization: " + key);
        }

        TableKey toTableKey() {
            if (this.secondaryKey == null) {
                return new TableKey(SERIALIZER.serialize(this.primaryKey));
            } else {
                return new TableKey(SERIALIZER.serialize(this.primaryKey), SERIALIZER.serialize(this.secondaryKey));
            }
        }

        @Override
        public String toString() {
            return this.secondaryKey == null ? this.primaryKey : String.format("%s:%s", this.primaryKey, this.secondaryKey);
        }
    }

    //region Create

    public static class Create extends KeyValueTableCommand {
        public Create(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureArgCount(3);
            val s = getScopedNameArg(0);
            val pkLength = getIntArg(1);
            val skLength = getIntArg(2);
            @Cleanup
            val m = createManager();
            val kvtConfig = KeyValueTableConfiguration.builder()
                    .partitionCount(getConfig().getDefaultSegmentCount())
                    .primaryKeyLength(pkLength)
                    .secondaryKeyLength(skLength)
                    .rolloverSizeBytes(getConfig().getRolloverSizeBytes())
                    .build();
            val success = m.createKeyValueTable(s.getScope(), s.getName(), kvtConfig);
            if (success) {
                output("Key-Value Table '%s/%s' created successfully.", s.getScope(), s.getName());
            } else {
                output("Key-Value Table '%s/%s' could not be created.", s.getScope(), s.getName());
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("create", "Creates one or more Key-Value Tables.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to create.")
                    .withArg("primary-key-length", "Length of primary keys (positive integer, less than 255).")
                    .withArg("secondary-key-length", "Length of secondary keys (non-negative integer, less than 255).")
                    .withSyntaxExample("scope1/kvt1 8 4", "Creates kvt1 in scope1 with primary key length 8 and secondary key length 4.")
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

        protected abstract void executeInternal(ScopedName kvtName, KeyValueTable kvt) throws Exception;

        @Override
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
        protected void executeInternal(ScopedName kvtName, KeyValueTable kvt) throws Exception {
            val keys = getJsonArg(1, String[].class);
            Preconditions.checkArgument(keys.length > 0, "Expected at least one key.");
            val tableKeys = Arrays.stream(keys)
                    .map(KeyInfo::parse)
                    .map(KeyInfo::toTableKey)
                    .collect(Collectors.toList());
            val result = kvt.getAll(tableKeys).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);

            output("Get %s Key(s) from %s:", keys.length, kvtName);
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
                    .withArg("keys", "A JSON Array representing the keys to get. Example: \"{[key1, key2, key3]}\".")
                    .withSyntaxExample("scope1/kvt1 {[key1, key2]}", "Gets 'key1' and 'key2' from scope1/kvt1 (where kvt1 has no secondary keys).")
                    .withSyntaxExample("scope1/kvt1 {[\"pk1:sk1\", \"pk2:sk2\"]}", "Gets 'pk1:sk1' and 'pk2:sk2' from scope1/kvt1.")
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
            ensureArgCount(3);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable kvt) throws Exception {
            val rawKey = getArg(1);
            val value = getArg(2);

            val key = KeyInfo.parse(rawKey);
            val version = kvt.update(new io.pravega.client.tables.Put(key.toTableKey(), SERIALIZER.serialize(value))).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            output("Key '%s' updated successfully. New version: '%s'.", key, version);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put", "Unconditionally inserts or updates a Table Entry.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to update.")
                    .withArg("key", "The key.")
                    .withArg("value", "The value.")
                    .withSyntaxExample("scope1/kvt1 key1 value1", "Sets 'key1:=value1' in 'scope1/kvt1' (where kvt1 has no secondary keys).")
                    .withSyntaxExample("scope1/kvt1 \"pk1:sk1\" value1", "Sets '[pk1, sk1]:=value1' in 'scope1/kvt1'.")
                    .build();
        }
    }

    public static class PutIf extends DataCommand {
        public PutIf(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(4);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable kvt) throws Exception {
            val rawKey = getArg(1);
            val version = Version.fromString(getArg(2));
            val value = getArg(3);

            val key = KeyInfo.parse(rawKey);
            val newVersion = kvt.update(new io.pravega.client.tables.Put(key.toTableKey(), SERIALIZER.serialize(value), version))
                    .get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            output("Key '%s' updated successfully. New version: '%s'.", key, newVersion);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put-if", "Conditionally inserts or updates a Table Entry.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to update.")
                    .withArg("key", "The key.")
                    .withArg("version", "The expected Key Version.")
                    .withArg("value", "The value.")
                    .withSyntaxExample("scope1/kvt1 key1 s1:1 value1", "Inserts 'key1:=value1' in 'scope1/kvt1', " +
                            "only if the current version of 'key1' is 's1:1'.")
                    .withSyntaxExample("scope1/kvt1 \"pk1:sk1\" s1:1 value1", "Inserts '[pk1, sk1]:=value1' in 'scope1/kvt1', " +
                            "only if the current version of '[pk1, sk1]' is 's1:1'.")
                    .build();
        }
    }

    public static class PutIfAbsent extends DataCommand {
        public PutIfAbsent(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected void ensurePreconditions() {
            ensureArgCount(3);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable kvt) throws Exception {
            val rawKey = getArg(1);
            val value = getArg(2);

            val key = KeyInfo.parse(rawKey);
            val version = kvt.update(new Insert(key.toTableKey(), SERIALIZER.serialize(value)))
                    .get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            output("Key '%s' inserted successfully. New version: '%s'.", key, version);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put-if-absent", "Inserts a Table Entry, only if its Key is not already present.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to update.")
                    .withArg("key", "The key.")
                    .withArg("value", "The value.")
                    .withSyntaxExample("scope1/kvt1 key1 value1", "Inserts 'key1:=value1' in 'scope1/kvt1', only if not already present.")
                    .withSyntaxExample("scope1/kvt1 \"pk1:sk1\" value1", "Inserts '[pk1, sk1]:=value1' in 'scope1/kvt1', only if not already present.")
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
            ensureArgCount(2);
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable kvt) throws Exception {
            val args = getJsonArg(1, String[][].class);
            val entries = toUpdates(args);
            Preconditions.checkArgument(entries.size() > 0, "Expected at least one Table Entry.");

            val result = kvt.update(entries).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            int conditionalCount = (int) entries.stream().filter(e -> e.getVersion() != Version.NO_VERSION).count();
            output("Updated %s Key(s) to %s (Conditional=%s, Unconditional=%s):",
                    entries.size(), kvtName, conditionalCount, entries.size() - conditionalCount);
            assert entries.size() == result.size() : String.format("Bad result length. Expected %s, actual %s", entries.size(), result.size());
            outputResultHeader("Key", "Version");
            for (int i = 0; i < result.size(); i++) {
                String[] output = toArray(new TableKey(entries.get(i).getKey().getPrimaryKey()));
                outputResult(output);
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("put-all", "Updates one or more Keys in a Key-Value table.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to update.")
                    .withArg("entries", "A JSON Array representing the keys to get.")
                    .withSyntaxExample("scope1/kvt1 {[[key1, value1]]}", "Unconditionally updates 'key1' to 'value1' in 'scope1/kvt1' (where kvt1 has no secondary keys).")
                    .withSyntaxExample("scope1/kvt1 {[[\"pk1:sk1\", value1]]}", "Unconditionally updates '[pk1, sk1]' to 'value1' in 'scope1/kvt1'.")
                    .withSyntaxExample("scope1/kvt1 {[[key1, \"seg1:ver1\", value1]]}",
                            "Conditionally updates 'key1' to 'value1' in 'scope1/kvt1' using 'seg1:ver1' as condition version.")
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
        protected void executeInternal(ScopedName kvtName, KeyValueTable kvt) throws Exception {
            val args = getJsonArg(1, String[][].class);
            val keys = toRemovals(args);
            Preconditions.checkArgument(keys.size() > 0, "Expected at least one Table Key.");
            kvt.update(keys).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);

            int conditionalCount = (int) keys.stream().filter(e -> e.getVersion() != Version.NO_VERSION).count();
            output("Removed %s Key(s) from %s (Conditional=%s, Unconditional=%s).",
                    keys.size(), kvtName, conditionalCount, keys.size() - conditionalCount);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("remove", "Removes one or more Keys from a Key-Value table.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to remove from.")
                    .withArg("[key-family]", "(Optional) Key Family to remove Keys for.")
                    .withArg("entries", "A JSON Array representing the keys to remove.")
                    .withSyntaxExample("scope1/kvt1 {[[key1]]}", "Unconditionally removes 'key1' from 'scope1/kvt1'.")
                    .withSyntaxExample("scope1/kvt1 {[[\"pk1:sk1\"]]}", "Unconditionally removes '[pk1, sk1]' from 'scope1/kvt1'.")
                    .withSyntaxExample("scope1/kvt1 {[[key1, \"s1:ver1\"]]}",
                            "Conditionally removes 'key1' from 'scope1/kvt1' using 'seg1:ver1' as condition version.")
                    .withSyntaxExample("scope1/kvt1 {[[key1, \"seg1:ver1\"], [key2]]}",
                            "Conditionally removes 'key1' and 'key2' from 'scope1/kvt1' " +
                                    "conditioned on `key1` having version 'seg1:ver1'.")
                    .build();
        }
    }

    //endregion

    //region Key/Entry iterators

    public static class ListEntries extends DataCommand {
        private static final String[] RESULT_HEADER = new String[]{"Key", "Version", "Value"};
        private static final String PK = "pk";
        private static final String GLOBAL_PREFIX = "prefix";
        private static final String GLOBAL_RANGE = "range";

        public ListEntries(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        protected void ensurePreconditions() {
            ensureMinArgCount(1);
        }

        @Override
        protected int getResultColumnCount() {
            return RESULT_HEADER.length;
        }

        @Override
        protected void executeInternal(ScopedName kvtName, KeyValueTable kvt) throws Exception {
            val builder = kvt.iterator().maxIterationSize(10);
            KeyValueTableIterator iterator = null;
            val argCount = getCommandArgs().getArgs().size();
            if (argCount <= 1) {
                // Global iterator.
                iterator = builder.all();
            } else {
                String type = getArg(1);
                if (type.equalsIgnoreCase(PK)) {
                    val pk = SERIALIZER.serialize(getArg(2));
                    if (argCount == 3) {
                        iterator = builder.forPrimaryKey(pk);
                    } else if (argCount == 4) {
                        val skPrefix = SERIALIZER.serialize(getArg(3));
                        iterator = builder.forPrimaryKey(pk, skPrefix);
                    } else if (argCount == 5) {
                        val sk1 = SERIALIZER.serialize(getArg(3));
                        val sk2 = SERIALIZER.serialize(getArg(4));
                        iterator = builder.forPrimaryKey(pk, sk1, sk2);
                    }
                } else if (type.equalsIgnoreCase(GLOBAL_PREFIX)) {
                    ensureArgCount(3);
                    val prefix = SERIALIZER.serialize(getArg(2));
                    iterator = builder.forPrefix(prefix);
                } else if (type.equalsIgnoreCase(GLOBAL_RANGE)) {
                    ensureArgCount(4);
                    val pk1 = SERIALIZER.serialize(getArg(2));
                    val pk2 = SERIALIZER.serialize(getArg(3));
                    iterator = builder.forRange(pk1, pk2);
                }
            }
            if (iterator == null) {
                throw new IllegalArgumentException("Invalid syntax.");
            }

            val entryIterator = iterator.entries();
            outputResultHeader(RESULT_HEADER);
            int count = 0;
            while (count < getConfig().getMaxListItems()) {
                val batch = entryIterator.getNext().get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
                if (batch == null) {
                    break; // We're done.
                }

                int maxCount = Math.min(getConfig().getMaxListItems() - count, batch.getItems().size());
                for (int i = 0; i < maxCount; i++) {
                    outputResult(toArray(batch.getItems().get(i)));
                }

                count += maxCount;
                if (maxCount < batch.getItems().size()) {
                    output("Only showing first %s items. Change this using '%s' config value.",
                            maxCount, InteractiveConfig.MAX_LIST_ITEMS);
                }
            }

            output("Total: %s item(s).", count);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("list-entries", "Lists all entries in a Key-Value Table.")
                    .withArg("scoped-kvt-name", "Name of the Scoped Key-Value Table to list entries from.")
                    .withArg("key-family", "Name of the Key Family to list entries from.")
                    .withArg("[type]", String.format("Type of iterator ('%s', '%s', '%s').", PK, GLOBAL_PREFIX, GLOBAL_RANGE))
                    .withSyntaxExample("scope1/kvt1 pk pk1", "Lists all entries 'scope1/kvt1' with primary key 'pk1'.")
                    .withSyntaxExample("scope1/kvt1 pk pk1 sk1 sk2", "Lists all entries 'scope1/kvt1' with primary key 'pk1' and secondary keys between 'sk1' and 'sk2'.")
                    .withSyntaxExample("scope1/kvt1 pk pk1 s", "Lists all entries 'scope1/kvt1' with primary key 'pk1' and secondary keys starting with 's'.")
                    .withSyntaxExample("scope1/kvt1 prefix p", "Lists all entries 'scope1/kvt1' with primary keys starting with 'p'.")
                    .withSyntaxExample("scope1/kvt1 range p1 p2", "Lists all entries 'scope1/kvt1' with primary keys between 'p1' and 'p2'.")
                    .withSyntaxExample("scope1/kvt1", "Lists all entries 'scope1/kvt1'.")
                    .build();
        }
    }

    //endregion

}
