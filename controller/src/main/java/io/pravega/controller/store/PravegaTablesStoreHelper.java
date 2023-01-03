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
package io.pravega.controller.store;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.pravega.client.tables.impl.HashTableIteratorItem;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.host.HostStoreException;
import io.pravega.controller.store.stream.Cache;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.util.RetryHelper;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.server.WireCommandFailedException.Reason.ConnectionDropped;
import static io.pravega.controller.server.WireCommandFailedException.Reason.ConnectionFailed;

/**
 * Helper class for all table related queries to segment store. This class invokes appropriate wire command calls into
 * SegmentHelper for all table related apis and then translates the failure responses to Store specific exceptions.
 */
public class PravegaTablesStoreHelper {
    public static final Function<Integer, byte[]> INTEGER_TO_BYTES_FUNCTION = x -> {
        byte[] bytes = new byte[Integer.BYTES];
        BitConverter.writeInt(bytes, 0, x);
        return bytes;
    };
    public static final Function<Long, byte[]> LONG_TO_BYTES_FUNCTION = x -> {
        byte[] bytes = new byte[Long.BYTES];
        BitConverter.writeLong(bytes, 0, x);
        return bytes;
    };
    public static final Function<UUID, byte[]> UUID_TO_BYTES_FUNCTION = x -> {
        byte[] b = new byte[2 * Long.BYTES];
        BitConverter.writeUUID(new ByteArraySegment(b), x);
        return b;
    };
    public static final Function<byte[], Long> BYTES_TO_LONG_FUNCTION = data -> BitConverter.readLong(data, 0);
    public static final Function<byte[], Integer> BYTES_TO_INTEGER_FUNCTION = x -> BitConverter.readInt(x, 0);
    public static final Function<byte[], UUID> BYTES_TO_UUID_FUNCTION = x -> BitConverter.readUUID(x, 0);
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaTablesStoreHelper.class));
    private static final int NUM_OF_RETRIES = 15; // approximately 1 minute worth of retries
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;
    private final Cache cache;
    private final AtomicReference<String> authToken;
    private final GrpcAuthHelper authHelper;
    private final int numOfRetries;

    @lombok.Data
    private static class TableCacheKey implements Cache.CacheKey {
        private final String table;
        private final String key;
    }

    public PravegaTablesStoreHelper(SegmentHelper segmentHelper, GrpcAuthHelper authHelper, ScheduledExecutorService executor) {
        this(segmentHelper, authHelper, executor, NUM_OF_RETRIES);
    }

    @VisibleForTesting
    PravegaTablesStoreHelper(SegmentHelper segmentHelper, GrpcAuthHelper authHelper, ScheduledExecutorService executor, int numOfRetries) {
        this.segmentHelper = segmentHelper;
        this.executor = executor;

        cache = new Cache();
        this.authHelper = authHelper;
        this.authToken = new AtomicReference<>(authHelper.retrieveMasterToken());
        this.numOfRetries = numOfRetries;
    }

    /**
     * Api to load a new value into the cache for the specified key from the requested table.
     * @param table name of table
     * @param key key to cache
     * @param <T> Type of object to deserialize the response into.
     */
    private  <T> void putInCache(String table, String key, VersionedMetadata<T> value, long time) {
        TableCacheKey cacheKey = new TableCacheKey(table, key);
        cache.put(cacheKey, value, time);
    }

    /**
     * Api to read cached value for the specified key from the requested table.
     * @param <T> Type of object to deserialize the response into.
     * @param table name of table
     * @param key key to query
     * @param time time after which the cache entry should have been loaded.
     * @param requestId request id
     * @return Returns a completableFuture which when completed will have the deserialized value with its store key version.
     */
    private <T> VersionedMetadata<T> getCachedData(String table, String key, long time, long requestId) {
        TableCacheKey cacheKey = new TableCacheKey(table, key);
        VersionedMetadata<?> cachedData = cache.getCachedData(cacheKey, time);
        if (cachedData == null) {
            return null;
        }
        log.trace(requestId, "found entry for key {} in table {} in cache", key, table);
        return getVersionedMetadata(cachedData);
    }

    /**
     * Method to invalidate cached value in the cache for the specified table.
     * @param table table name
     * @param key key to invalidate
     */
    public void invalidateCache(String table, String key) {
        cache.invalidateCache(new TableCacheKey(table, key));
    }

    @SuppressWarnings("unchecked")
    private <T> VersionedMetadata<T> getVersionedMetadata(VersionedMetadata<?> v) {
        // Since cache is untyped and holds all types of deserialized objects, we typecast it to the requested object type
        // based on the type in caller's supplied Deserialization function.
        return new VersionedMetadata<>((T) v.getObject(), v.getVersion());
    }

    /**
     * Method to create a new Table. If the table already exists, segment helper responds with success.
     * @param tableName table name
     * @param requestId request id
     * @return CompletableFuture which when completed will indicate successful creation of table.
     */
    public CompletableFuture<Void> createTable(String tableName, long requestId) {
        return this.createTable(tableName, requestId, 0);
    }

    /**
     * Method to create a new Table. If the table already exists, segment helper responds with success.
     * @param tableName table name
     * @param requestId request id
     * @param rolloverSizeBytes rollover size of the table segment
     * @return CompletableFuture which when completed will indicate successful creation of table.
     */
    public CompletableFuture<Void> createTable(String tableName, long requestId, long rolloverSizeBytes) {
        log.info(requestId, "create table called for table: {}", tableName);

        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(tableName, authToken.get(), requestId,
                false, 0, rolloverSizeBytes),
                () -> String.format("create table: %s", tableName), requestId))
                .whenCompleteAsync((r, e) -> {
                    if (e != null) {
                        log.warn(requestId, "create table {} threw exception", tableName, e);
                    } else {
                        log.debug(requestId, "table {} created successfully", tableName);
                    }
                }, executor);
    }

    /**
     * Method to delete a table. The callers can supply a `mustBeEmpty` flag and the table is deleted only if it is empty.
     * Note: the mustBeEmpty flag is passed to segment store via segment helper and it is responsibility of segment store
     * table implementation to delete a table only if it is empty.
     * @param tableName tableName
     * @param mustBeEmpty flag to indicate to table store to delete table only if it is empty.
     * @param requestId request id
     * @return CompletableFuture which when completed will indicate that the table was deleted successfully.
     * If mustBeEmpty is set to true and the table is non-empty then the future is failed with StoreException.DataNotEmptyException
     */
    public CompletableFuture<Void> deleteTable(String tableName, boolean mustBeEmpty, long requestId) {
        log.debug(requestId, "delete table called for table: {}", tableName);
        return expectingDataNotFound(withRetries(() -> segmentHelper.deleteTableSegment(
                tableName, mustBeEmpty, authToken.get(), requestId),
                () -> String.format("delete table: %s", tableName), requestId), null)
                .thenAcceptAsync(v -> log.debug(requestId, "table {} deleted successfully", tableName), executor);
    }

    /**
     * Method to add new entry to a table.
     * @param tableName table name
     * @param key key to add
     * @param toBytes function to convert value to bytes
     * @param val value to add
     * @param <T> Type of value to be added
     * @param requestId request id
     * @return CompletableFuture which when completed will have the version of the key returned by segment store.
     * If the key already exists, it will throw StoreException.DataExistsException.
     */
    public <T> CompletableFuture<Version> addNewEntry(String tableName, String key, T val, Function<T, byte[]> toBytes,
                                                      long requestId) {
        log.info(requestId, "addNewEntry called for : {} key : {}", tableName, key);
        byte[] value = toBytes.apply(val);
        List<TableSegmentEntry> entries = Collections.singletonList(
                TableSegmentEntry.notExists(key.getBytes(Charsets.UTF_8), value));
        Supplier<String> errorMessage = () -> String.format("addNewEntry: key: %s table: %s", key, tableName);
        long time = System.currentTimeMillis();
        return withRetries(() -> segmentHelper.updateTableEntries(tableName, entries, authToken.get(), requestId),
                errorMessage, true, requestId)
                .exceptionally(e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    invalidateCache(tableName, key);

                    if (unwrap instanceof StoreException.WriteConflictException) {
                        throw StoreException.create(StoreException.Type.DATA_EXISTS, errorMessage.get());
                    } else {
                        log.debug(requestId, "add new entry {} to {} threw exception {} {}",
                                key, tableName, unwrap.getClass(), unwrap.getMessage());
                        throw new CompletionException(e);
                    }
                })
                .thenApplyAsync(x -> {
                    TableSegmentKeyVersion first = x.get(0);
                    log.debug(requestId, "entry for key {} added to table {} with version {}",
                            key, tableName, first.getSegmentVersion());
                    Version version = new Version.LongVersion(first.getSegmentVersion());
                    putInCache(tableName, key, new VersionedMetadata<>(val, version), time);
                    return version;
                }, executor)
                .whenComplete((r, ex) -> releaseEntries(entries));
    }

    /*
        Conditional delete of the specified key with the specified version.
        If the conditional delete fails it is ignored.
     */
    private CompletableFuture<Void> conditionalDeleteOfKey(String tableName, long requestId, String key,
                                                           TableSegmentKeyVersion keyVersion) {
        return expectingWriteConflict(removeEntry(tableName, key, new Version.LongVersion(keyVersion.getSegmentVersion()), requestId), null);
    }

    /**
     * Method to get the number of entries in a Table Segment. If the table already exists, segment helper responds with success.
     * @param tableName Name of the Table Segment for which we want the entry count
     * @param requestId request id
     * @return CompletableFuture which when completed will return number of entries in table.
     */
    public CompletableFuture<Long> getEntryCount(String tableName, long requestId) {
        log.debug(requestId, "create table called for table: {}", tableName);

        return withRetries(() -> segmentHelper.getTableSegmentEntryCount(tableName, authToken.get(), requestId),
                () -> String.format("GetInfo table: %s", tableName), requestId)
                .whenCompleteAsync((r, e) -> {
                    if (e != null) {
                        log.warn(requestId, "Get Table Segment info for table {} threw exception", tableName, e);
                    } else {
                        log.debug(requestId, "Get Table Segment info for table {} completed successfully", tableName);
                    }
                }, executor);
    }

    /**
     * Method to get and conditionally update value for the specified key.
     *
     * This method fetches the latest version of the value for the specified key and applies the update function and
     * updates the value.
     *
     * @param tableName Table Name.
     * @param tableKey Key to update.
     * @param updateFunction A function which updates the values.
     * @param attemptCleanup A Predicate which decides if a cleanup should be invoked.
     * @param requestId Request id.
     * @return A future which completes when the update operation has completed. A conditional update failure will cause
     * the future to complete exceptionally.
     */
    public CompletableFuture<Void> getAndUpdateEntry(final String tableName, final String tableKey, final Function<TableSegmentEntry, TableSegmentEntry> updateFunction,
                                                     final Predicate<TableSegmentEntry> attemptCleanup, long requestId) {
        Supplier<String> errorMessage = () -> String.format("get and update values: on table: %s for key %s", tableName, tableKey);
        // fetch un-versioned key
        List<TableSegmentKey> keys = Collections.singletonList(TableSegmentKey.unversioned(tableKey.getBytes(StandardCharsets.UTF_8)));
        return withRetries(() -> segmentHelper.readTable(tableName, keys, authToken.get(), requestId)
                                              .whenComplete((v, ex) -> releaseKeys(keys))
                                              .thenCompose(entries -> {
                                                  // apply update.
                                                  TableSegmentEntry updatedEntry = updateFunction.apply(entries.get(0));
                                                  // check if a conditional delete should be performed post updation.
                                                  boolean shouldAttemptCleanup = attemptCleanup.test(updatedEntry);
                                                  return segmentHelper.updateTableEntries(tableName, Collections.singletonList(updatedEntry), authToken.get(), requestId)
                                                                      .thenCompose(keyVersions -> {
                                                                          if (shouldAttemptCleanup) {
                                                                              log.debug(requestId, "Delete of table key {} on table {}", tableKey, tableName);
                                                                              // attempt a conditional delete of the entry since there are zero entries.
                                                                              return conditionalDeleteOfKey(tableName, requestId, tableKey, keyVersions.get(0));
                                                                          } else {
                                                                              return CompletableFuture.completedFuture(null);
                                                                          }
                                                                      }).whenComplete((v, ex) -> releaseEntries(Collections.singletonList(updatedEntry)));
                                              }), errorMessage, true, requestId);

    }

    /**
     * Method to add new entry to table if it does not exist.
     * @param tableName tableName
     * @param key Key to add
     * @param toBytes function to convert value to bytes.
     * @param val value to add
     * @param <T> Type of value to be added
     * @param requestId request id
     * @return CompletableFuture which when completed will have added entry to the table if it did not exist.
     */
    public <T> CompletableFuture<Version> addNewEntryIfAbsent(String tableName, String key, T val, Function<T, byte[]> toBytes,
                                                              long requestId) {
        // if entry exists, we will get write conflict in attempting to create it again.
        return expectingDataExists(addNewEntry(tableName, key, val, toBytes, requestId), null);
    }

    /**
     * Method to add a batch of entries if absent. Table implementation on segment store guarantees that either all or none of
     * the entries are added.
     * If segment store responds with success, then it is guaranteed that all entries are added to the store.
     * However, it is important to note that the segment store could respond with Data Exists even if one of the entries exists.
     * In such case, this method will ignore data exist and respond with success for the entire batch. It does not verify
     * if all entries existed or one of the entries existed.
     * Callers should use this only if they are guaranteed to never create the requested entries outside of the requested batch.
     *
     * @param tableName table name
     * @param toAdd map of keys and values to add.
     * @param toBytes function to serialize individual values in the list of items to add.
     * @param requestId request id
     * @param <T>       type of values.
     * @return CompletableFuture which when completed successfully will indicate that all entries have been added successfully.
     */
    public <T> CompletableFuture<Void> addNewEntriesIfAbsent(String tableName, List<Map.Entry<String, T>> toAdd,
                                                             Function<T, byte[]> toBytes, long requestId) {
        List<TableSegmentEntry> entries = toAdd.stream().map(x ->
                TableSegmentEntry.notExists(x.getKey().getBytes(Charsets.UTF_8), toBytes.apply(x.getValue())))
                                               .collect(Collectors.toList());
        Supplier<String> errorMessage = () -> String.format("addNewEntriesIfAbsent: table: %s", tableName);
        long time = System.currentTimeMillis();
        return expectingDataExists(withRetries(() -> segmentHelper.updateTableEntries(tableName, entries, authToken.get(),
                requestId), errorMessage, requestId)
                .handle((r, e) -> {
                    releaseEntries(entries);
                    if (e != null) {
                        Throwable unwrap = Exceptions.unwrap(e);
                        toAdd.forEach(entry -> invalidateCache(tableName, entry.getKey()));
                        if (unwrap instanceof StoreException.WriteConflictException) {
                            throw StoreException.create(StoreException.Type.DATA_EXISTS, errorMessage.get());
                        } else {
                            log.debug(requestId, "add new entries to {} threw exception {} {}",
                                    tableName, unwrap.getClass(), unwrap.getMessage());
                            throw new CompletionException(e);
                        }
                    } else {
                        log.debug(requestId, "entries added {} to table {}", toAdd, tableName);
                        for (int i = 0; i < r.size(); i++) {
                            putInCache(tableName, toAdd.get(i).getKey(),
                                    new VersionedMetadata<>(toAdd.get(i).getValue(),
                                            new Version.LongVersion(r.get(i).getSegmentVersion())), time);
                        }
                        return null;
                    }
                }), null);
    }

    /**
     * Method to update a single entry.
     * @param tableName tablename
     * @param key key
     * @param toBytes to bytes function
     * @param val value
     * @param ver previous key version
     * @param <T> Type of value to be added
     * @param requestId request id
     * @return CompletableFuture which when completed will indicate that the value is updated in the table.
     */
    public <T> CompletableFuture<Version> updateEntry(String tableName, String key, T val, Function<T, byte[]> toBytes,
                                                      Version ver, long requestId) {
        long version = ver.asLongVersion().getLongValue();
        log.trace(requestId, "updateEntry entry called for : {} key : {} version {}", tableName, key, version);
        byte[] value = toBytes.apply(val);
        long time = System.currentTimeMillis();
        List<TableSegmentEntry> entries = Collections.singletonList(
                TableSegmentEntry.versioned(key.getBytes(Charsets.UTF_8), value, version));
        return withRetries(() -> segmentHelper.updateTableEntries(tableName, entries, authToken.get(), requestId),
                () -> String.format("updateEntry: key: %s table: %s", key, tableName), true, requestId)
                .thenApplyAsync(x -> {
                    TableSegmentKeyVersion first = x.get(0);
                    log.debug(requestId, "entry for key {} updated to table {} with new version {}",
                            key, tableName, first.getSegmentVersion());
                    Version newVersion = new Version.LongVersion(first.getSegmentVersion());
                    putInCache(tableName, key, new VersionedMetadata<>(val, newVersion), time);
                    return newVersion;
                }, executor)
                .exceptionally(e -> {
                    invalidateCache(tableName, key);
                    throw new CompletionException(e);
                })
                .whenComplete((r, ex) -> releaseEntries(entries));
    }

    /**
     * Method to retrieve the value for a given key from a table. This method takes a deserialization function and deserializes
     * the received byte[] using the supplied function.
     * @param tableName tableName
     * @param key key
     * @param fromBytes deserialization function
     * @param <T> Type of deserialized object
     * @param requestId request id
     * @return CompletableFuture which when completed will have the versionedMetadata retrieved from the store.
     */
    public <T> CompletableFuture<VersionedMetadata<T>> getEntry(String tableName, String key, Function<byte[], T> fromBytes,
                                                                long requestId) {
        log.trace(requestId, "get entry called for : {} key : {}", tableName, key);
        List<TableSegmentKey> keys = Collections.singletonList(TableSegmentKey.unversioned(key.getBytes(Charsets.UTF_8)));
        CompletableFuture<VersionedMetadata<T>> result = new CompletableFuture<>();
        String message = "get entry: key: %s table: %s";
        withRetries(() -> segmentHelper.readTable(tableName, keys, authToken.get(), requestId),
                () -> String.format(message, key, tableName), requestId)
                .thenApplyAsync(x -> {
                    try {
                        TableSegmentEntry first = x.get(0);
                        if (first.getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)) {
                            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, String.format(message, key, tableName));
                        } else {
                            log.trace(requestId, "returning entry for : {} key : {} with version {}", tableName, key,
                                    first.getKey().getVersion().getSegmentVersion());

                            T deserialized = fromBytes.apply(getArray(first.getValue()));

                            return new VersionedMetadata<>(deserialized,
                                    new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
                        }
                    } finally {
                        releaseEntries(x);
                    }
                }, executor)
                .whenCompleteAsync((r, e) -> {
                    releaseKeys(keys);
                    if (e != null) {
                        result.completeExceptionally(e);
                    } else {
                        result.complete(r);
                    }
                }, executor);
        return result;
    }

    /**
     * Method to retrieve the value for a given key from a table and it fetches from cache if the aftertime is less than
     * the time of the cached value. After it retrieves the value, it loads it into the cache.
     * loads it into the cache. This method takes a deserialization function and deserializes
     * the received byte[] using the supplied function.
     * @param tableName tableName
     * @param key key
     * @param fromBytes deserialization function
     * @param <T> Type of deserialized object
     * @param afterTime time after which the cache entry should be treated as valid. if the value was loaded before this time,
     *                  then ignore cached and fetch from store.
     * @param requestId request id
     * @return CompletableFuture which when completed will have the versionedMetadata retrieved from the store.
     */
    public <T> CompletableFuture<VersionedMetadata<T>> getCachedOrLoad(String tableName, String key, Function<byte[], T> fromBytes,
                                                                       long afterTime, long requestId) {
        log.trace(requestId, "get entry called for : {} key : {}", tableName, key);
        VersionedMetadata<Object> cached = getCachedData(tableName, key, afterTime, requestId);
        if (cached != null) {
            return CompletableFuture.completedFuture(getVersionedMetadata(cached));
        } else {
            long time = System.currentTimeMillis();
            return getEntry(tableName, key, fromBytes, requestId)
                    .thenApply(r -> {
                        putInCache(tableName, key, r, time);
                        return r;
                    });
        }
    }

    /**
     * Method to retrieve the value for a given key from a table. This method takes a deserialization function and deserializes
     * the received byte[] using the supplied function.
     * @param tableName tableName
     * @param keys keys to read
     * @param fromBytes deserialization function
     * @param nonExistent entry to populate for non existent keys
     * @param <T> Type of deserialized object
     * @param requestId request id
     * @return CompletableFuture which when completed will have the versionedMetadata retrieved from the store.
     */
    public <T> CompletableFuture<List<VersionedMetadata<T>>> getEntries(String tableName, List<String> keys, 
                                                                        Function<byte[], T> fromBytes,
                                                                        VersionedMetadata<T> nonExistent,
                                                                        long requestId) {
        log.trace(requestId, "get entries called for : {} keys : {}", tableName, keys);
        List<TableSegmentKey> tableKeys = keys.stream().map(key -> TableSegmentKey.unversioned(key.getBytes(Charsets.UTF_8)))
                                              .collect(Collectors.toList());
        CompletableFuture<List<VersionedMetadata<T>>> result = new CompletableFuture<>();

        String message = "get entry: key: %s table: %s";
        long time = System.currentTimeMillis();

        withRetries(() -> segmentHelper.readTable(tableName, tableKeys, authToken.get(), requestId),
                () -> String.format(message, keys, tableName), requestId)
                .thenApplyAsync(entries -> {
                    try {
                        List<VersionedMetadata<T>> list = new ArrayList<>(keys.size());
                        for (int i = 0; i < keys.size(); i++) {
                            TableSegmentEntry entry = entries.get(i);
                            if (entry.getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)) {
                                list.add(nonExistent);
                            } else {
                                VersionedMetadata<T> tVersionedMetadata = new VersionedMetadata<>(
                                        fromBytes.apply(getArray(entry.getValue())),
                                        new Version.LongVersion(entry.getKey().getVersion().getSegmentVersion()));
                                putInCache(tableName, keys.get(i), tVersionedMetadata, time);
                                list.add(tVersionedMetadata);
                            }
                        }
                        return list;
                    } finally {
                        releaseEntries(entries);
                    }
                }, executor)
                .whenCompleteAsync((r, e) -> {
                    releaseKeys(tableKeys);
                    if (e != null) {
                        result.completeExceptionally(e);
                    } else {
                        result.complete(r);
                    }
                }, executor);
        
        return result;
    }

    /**
     * Method to remove entry from the store.
     * @param tableName tableName
     * @param key key
     * @param requestId request id
     * @return CompletableFuture which when completed will indicate successful deletion of entry from the table.
     * It ignores DataNotFound exception.
     */
    public CompletableFuture<Void> removeEntry(String tableName, String key, long requestId) {
        return removeEntry(tableName, key, null, requestId);
    }

    /**
     * Method to remove entry from the store. 
     * @param tableName tableName
     * @param key key
     * @param ver version for conditional removal
     * @param requestId request id
     * @return CompletableFuture which when completed will indicate successful deletion of entry from the table.
     * It ignores DataNotFound exception. 
     */
    public CompletableFuture<Void> removeEntry(String tableName, String key, Version ver, long requestId) {
        log.trace(requestId, "remove entry called for : {} key : {}", tableName, key);
        TableSegmentKey tableKey = ver == null
                ? TableSegmentKey.unversioned(key.getBytes(Charsets.UTF_8))
                : TableSegmentKey.versioned(key.getBytes(Charsets.UTF_8), ver.asLongVersion().getLongValue());

        return expectingDataNotFound(withRetries(() -> segmentHelper.removeTableKeys(
                tableName, Collections.singletonList(tableKey), authToken.get(), requestId),
                () -> String.format("remove entry: key: %s table: %s", key, tableName), requestId), null)
                .thenAcceptAsync(v -> {
                    invalidateCache(tableName, key);
                    log.trace(requestId, "entry for key {} removed from table {}", key, tableName);
                }, executor)
                .whenComplete((r, ex) -> releaseKeys(Collections.singleton(tableKey)));
    }

    /**
     * Removes a batch of entries from the table store. Ignores data not found exception and treats it as success.
     * If table store throws dataNotFound for a subset of entries, there is no way for this method to disambiguate.
     * So it is the responsibility of the caller to use this api if they are guaranteed to always attempt to
     * remove same batch entries.
     * @param tableName table name
     * @param keys keys to delete
     * @param requestId request id
     * @return CompletableFuture which when completed will have entries removed from the table.
     */
    public CompletableFuture<Void> removeEntries(String tableName, Collection<String> keys, long requestId) {
        log.trace(requestId, "remove entry called for : {} keys : {}", tableName, keys);

        List<TableSegmentKey> listOfKeys = keys.stream()
                                               .map(x -> TableSegmentKey.unversioned(x.getBytes(Charsets.UTF_8)))
                                               .collect(Collectors.toList());
        return expectingDataNotFound(withRetries(() -> segmentHelper.removeTableKeys(
                tableName, listOfKeys, authToken.get(), requestId),
                () -> String.format("remove entries: keys: %s table: %s", keys.toString(), tableName), requestId), null)
                .thenAcceptAsync(v -> {
                    keys.forEach(key -> invalidateCache(tableName, key));
                    log.trace(requestId, "entry for keys {} removed from table {}", keys, tableName);
                }, executor)
                .whenComplete((r, ex) -> releaseKeys(listOfKeys));
    }

    /**
     * Method to get paginated list of keys with a continuation token.
     * @param tableName tableName
     * @param continuationToken previous continuationToken
     * @param limit limit on number of keys to retrieve
     * @param requestId request id
     * @return CompletableFuture which when completed will have a list of keys of size less than or equal to limit and
     * a new ContinutionToken.
     */
    public CompletableFuture<Map.Entry<ByteBuf, List<String>>> getKeysPaginated(String tableName, ByteBuf continuationToken,
                                                                                int limit, long requestId) {
        log.trace(requestId, "get keys paginated called for : {}", tableName);
        return withRetries(
                () -> segmentHelper.readTableKeys(tableName, limit, HashTableIteratorItem.State.fromBytes(continuationToken),
                        authToken.get(), requestId),
                () -> String.format("get keys paginated for table: %s", tableName), requestId)
                .thenApplyAsync(result -> {
                    try {
                        List<String> items = result.getItems().stream().map(x -> new String(getArray(x.getKey()),
                                Charsets.UTF_8)).collect(Collectors.toList());
                        log.trace(requestId, "get keys paginated on table {} returned items {}", tableName, items);
                        // if the returned token and result are empty, return the incoming token so that
                        // callers can resume from that token.
                        return new AbstractMap.SimpleEntry<>(getNextToken(continuationToken, result), items);
                    } finally {
                        releaseKeys(result.getItems());
                    }
                }, executor);
    }

    /**
     * Method to get paginated list of entries with a continuation token.
     * @param tableName tableName
     * @param continuationToken previous continuationToken
     * @param limit limit on number of entries to retrieve
     * @param fromBytes function to deserialize byte array into object of type T
     * @param <T> type of deserialized entry values
     * @param requestId request id
     * @return CompletableFuture which when completed will have a list of entries of size less than or equal to limit and
     * a new ContinutionToken.
     */
    public <T> CompletableFuture<Map.Entry<ByteBuf, List<Map.Entry<String, VersionedMetadata<T>>>>> getEntriesPaginated(
            String tableName, ByteBuf continuationToken, int limit,
            Function<byte[], T> fromBytes, long requestId) {
        log.trace(requestId, "get entries paginated called for : {}", tableName);
        long time = System.currentTimeMillis();
        return withRetries(() -> segmentHelper.readTableEntries(tableName, limit,
                HashTableIteratorItem.State.fromBytes(continuationToken), authToken.get(), requestId),
                () -> String.format("get entries paginated for table: %s", tableName), requestId)
                .thenApplyAsync(result -> {
                    try {
                        List<Map.Entry<String, VersionedMetadata<T>>> items = result.getItems().stream().map(x -> {
                            String key = new String(getArray(x.getKey().getKey()), Charsets.UTF_8);
                            T deserialized = fromBytes.apply(getArray(x.getValue()));
                            VersionedMetadata<T> value = new VersionedMetadata<>(deserialized, new Version.LongVersion(
                                    x.getKey().getVersion().getSegmentVersion()));
                            putInCache(tableName, key, value, time);
                            return new AbstractMap.SimpleEntry<>(key, value);
                        }).collect(Collectors.toList());
                        log.trace(requestId, "get keys paginated on table {} returned number of items {}", tableName, items.size());
                        // if the returned token and result are empty, return the incoming token so that 
                        // callers can resume from that token. 
                        return new AbstractMap.SimpleEntry<>(getNextToken(continuationToken, result), items);
                    } finally {
                        releaseEntries(result.getItems());
                    }
                }, executor);
    }

    private ByteBuf getNextToken(ByteBuf continuationToken, HashTableIteratorItem<?> result) {
        return result.getItems().isEmpty() && result.getState().isEmpty() ?
                continuationToken : Unpooled.wrappedBuffer(result.getState().toBytes());
    }

    /**
     * Method to retrieve all keys in the table. It returns an asyncIterator which can be used to iterate over the returned keys.
     * @param tableName table name
     * @param requestId request id
     * @return AsyncIterator that can be used to iterate over keys in the table.
     */
    public AsyncIterator<String> getAllKeys(String tableName, long requestId) {
        return new ContinuationTokenAsyncIterator<>(token -> getKeysPaginated(tableName, token, 1000, requestId)
                .thenApplyAsync(result -> {
                    token.release();
                    return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                }, executor),
                HashTableIteratorItem.State.EMPTY.getToken());
    }

    /**
     * Method to retrieve all entries in the table. It returns an asyncIterator which can be used to iterate over the returned entries.
     * @param tableName table name
     * @param fromBytes deserialization function to deserialize returned value.
     * @param <T> Type of value
     * @param requestId request id
     * @return AsyncIterator that can be used to iterate over keys in the table.
     */
    public <T> AsyncIterator<Map.Entry<String, VersionedMetadata<T>>> getAllEntries(String tableName, Function<byte[], T> fromBytes,
                                                                                    long requestId) {
        return new ContinuationTokenAsyncIterator<>(token -> getEntriesPaginated(tableName, token, 1000, fromBytes,
                requestId)
                .thenApplyAsync(result -> {
                    token.release();
                    return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                }, executor),
                HashTableIteratorItem.State.EMPTY.getToken());
    }

    public <T> CompletableFuture<T> expectingDataNotFound(CompletableFuture<T> future, T toReturn) {
        return Futures.exceptionallyExpecting(future, e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException,
                toReturn);
    }

    public <T> CompletableFuture<T> expectingWriteConflict(CompletableFuture<T> future, T toReturn) {
        return Futures.exceptionallyExpecting(future, e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException,
                                              toReturn);
    }

    <T> CompletableFuture<T> expectingDataExists(CompletableFuture<T> future, T toReturn) {
        return Futures.exceptionallyExpecting(future, e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException,
                toReturn);
    }

    private <T> Supplier<CompletableFuture<T>> exceptionalCallback(Supplier<CompletableFuture<T>> future,
                                                                   Supplier<String> errorMessageSupplier,
                                                                   boolean throwOriginalOnCFE,
                                                                   long requestId) {
        return () -> CompletableFuture.completedFuture(null).thenComposeAsync(v -> future.get(), executor).exceptionally(t -> {
            String errorMessage = errorMessageSupplier.get();
            Throwable cause = Exceptions.unwrap(t);
            Throwable toThrow;
            if (cause instanceof WireCommandFailedException) {
                WireCommandFailedException wcfe = (WireCommandFailedException) cause;
                switch (wcfe.getReason()) {
                    case ConnectionDropped:
                    case ConnectionFailed:
                        toThrow = throwOriginalOnCFE ? wcfe :
                                StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case UnknownHost:
                        toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case PreconditionFailed:
                        toThrow = StoreException.create(StoreException.Type.ILLEGAL_STATE, wcfe, errorMessage);
                        break;
                    case AuthFailed:
                        authToken.set(authHelper.retrieveMasterToken());
                        toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case SegmentDoesNotExist:
                        toThrow = StoreException.create(StoreException.Type.DATA_CONTAINER_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableSegmentNotEmpty:
                        toThrow = StoreException.create(StoreException.Type.DATA_CONTAINS_ELEMENTS, wcfe, errorMessage);
                        break;
                    case TableKeyDoesNotExist:
                        toThrow = StoreException.create(StoreException.Type.DATA_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableKeyBadVersion:
                        toThrow = StoreException.create(StoreException.Type.WRITE_CONFLICT, wcfe, errorMessage);
                        break;
                    default:
                        toThrow = StoreException.create(StoreException.Type.UNKNOWN, wcfe, errorMessage);
                }
            } else if (cause instanceof HostStoreException) {
                log.warn(requestId, "Host Store exception {}", cause.getMessage());
                toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, cause, errorMessage);
            } else {
                log.warn(requestId, "exception of unknown type thrown {} ", errorMessage, cause);
                toThrow = StoreException.create(StoreException.Type.UNKNOWN, cause, errorMessage);
            }

            throw new CompletionException(toThrow);
        });
    }

    /*
     * We don't want to do indefinite retries because for controller's graceful shutting down, it waits on grpc service to
     * be terminated which in turn waits on all outstanding grpc calls to complete. And the store may stall the calls if
     * there is indefinite retries. Restricting it to 12 retries gives us ~60 seconds worth of wait on the upper side.
     * Also, note that the call can fail because hostContainerMap has not been updated or it can fail because it cannot
     * talk to segment store. Both these are translated to ConnectionErrors and are retried. All other exceptions
     * are thrown back
     */
    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Supplier<String> errorMessage,
                                                 long requestId) {
        return withRetries(futureSupplier, errorMessage, false, requestId);
    }

    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Supplier<String> errorMessage,
                                         boolean throwOriginalOnCfe, long requestId) {
        return RetryHelper.withRetriesAsync(exceptionalCallback(futureSupplier, errorMessage, throwOriginalOnCfe, requestId),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException, numOfRetries, executor)
                .exceptionally(e -> {
                    Throwable t = Exceptions.unwrap(e);
                    if (t instanceof RetriesExhaustedException) {
                        throw new CompletionException(t.getCause());
                    } else {
                        Throwable unwrap = Exceptions.unwrap(e);
                        if (unwrap instanceof WireCommandFailedException &&
                                (((WireCommandFailedException) unwrap).getReason().equals(ConnectionDropped) ||
                                        ((WireCommandFailedException) unwrap).getReason().equals(ConnectionFailed))) {
                            throw new CompletionException(StoreException.create(StoreException.Type.CONNECTION_ERROR,
                                    errorMessage.get()));
                        } else {
                            throw new CompletionException(unwrap);
                        }
                    }
                });
    }

    byte[] getArray(ByteBuf buf) {
        final byte[] bytes = new byte[buf.readableBytes()];
        final int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        return bytes;
    }

    private void releaseKeys(Collection<TableSegmentKey> keys) {
        for (TableSegmentKey k : keys) {
            ReferenceCountUtil.safeRelease(k.getKey());
        }
    }

    private void releaseEntries(Collection<TableSegmentEntry> entries) {
        for (TableSegmentEntry e : entries) {
            ReferenceCountUtil.safeRelease(e.getKey().getKey());
            ReferenceCountUtil.safeRelease(e.getValue());
        }
    }

    /**
     * Helper method to load the value from a table into the cache. It first attempts to
     * load the value by using the table name which may already be cached.
     * It handles table (data container) not found exception
     * and automatically invokes tablename supplier with "ignore cached" value to fetch the latest table name and uses
     * that to read the specified key and load the value into the cache and return the value to the caller.
     *
     * @param tableNameSupplier a bifunction tht takes a flag for whether to ignore cached table name.
     *                         It also takes an operation context and returns a table name.
     * @param key Key to load
     * @param valueFunction deserializer for value.
     * @param context operation context
     * @param <T> type of value.
     * @return CompletableFuture which when completed will hold the value which is loaded into the cache.
     */
    public <T> CompletableFuture<VersionedMetadata<T>> loadFromTableHandleStaleTableName(
            BiFunction<Boolean, OperationContext, CompletableFuture<String>> tableNameSupplier,
                         String key, Function<byte[], T> valueFunction, OperationContext context) {
        return Futures.exceptionallyComposeExpecting(
                tableNameSupplier.apply(false, context).thenCompose(tableName ->
                        getCachedOrLoad(tableName, key,
                                valueFunction, context.getOperationStartTime(), context.getRequestId())),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataContainerNotFoundException,
                () -> tableNameSupplier.apply(true, context).thenCompose(tableName ->
                        getCachedOrLoad(tableName, key,
                                valueFunction, context.getOperationStartTime(), context.getRequestId())));
    }
}
