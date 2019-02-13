package io.pravega.controller.store.stream;

import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.util.AsyncIterator;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.stream.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PravegaTablesStoreHelper {
    private final SegmentHelper segmentHelper;

    public PravegaTablesStoreHelper(SegmentHelper segmentHelper) {
        this.segmentHelper = segmentHelper;
    }
    
    CompletableFuture<Boolean> createTable(String scope, String tableName) {
        return segmentHelper.createTableSegment(scope, tableName, RequestTag.NON_EXISTENT_ID);
    }

    CompletableFuture<Data> addNewEntry(String scope, String tableName, String key, byte[] value) {
        List<TableEntry<byte[], byte[]>> entries = new LinkedList<>();
        TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(), KeyVersion.NOT_EXISTS), value);
        entries.add(entry);
        return segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID)
                .thenApply(x -> {
                    KeyVersion first = x.get(0);
                    return new Data(value, new Version.LongVersion(first.getSegmentVersion()));
                });
    }

    CompletableFuture<Data> updateEntry(String scope, String tableName, String key, Data value) {
        List<TableEntry<byte[], byte[]>> entries = new LinkedList<>();
        KeyVersionImpl version = new KeyVersionImpl(value.getVersion().asLongVersion().getLongValue());
        TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(), version), value.getData());
        entries.add(entry);
        return segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID)
                .thenApply(x -> {
                    KeyVersion first = x.get(0);
                    return new Data(value.getData(), new Version.LongVersion(first.getSegmentVersion()));
                });
    }

    CompletableFuture<Data> getEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new LinkedList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return segmentHelper.readTable(scope, tableName, keys, RequestTag.NON_EXISTENT_ID)
                .thenApply(x -> {
                    TableEntry<byte[], byte[]> first = x.get(0);
                    return new Data(first.getValue(), new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
                });
    }

    CompletableFuture<Void> removeEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new LinkedList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return segmentHelper.removeTableKeys(scope, tableName, keys, 0L);
    }
    
    CompletableFuture<AsyncIterator<String>> getAllKeys(String scope, String tableName) {
        throw new UnsupportedOperationException();
    }

    CompletableFuture<AsyncIterator<Pair<String, Data>>> getAllEntries(String scope, String tableName) {
        throw new UnsupportedOperationException();
    }
}
