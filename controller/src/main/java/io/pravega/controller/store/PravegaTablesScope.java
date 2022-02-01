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

import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.HashHelper;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.records.TagRecord;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.controller.store.PravegaTablesStoreHelper.BYTES_TO_UUID_FUNCTION;
import static io.pravega.controller.store.PravegaTablesStoreHelper.UUID_TO_BYTES_FUNCTION;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SCOPES_TABLE;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.SEPARATOR;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * Pravega Tables based scope metadata.
 * At top level, there is a common scopes table at _system. This has a list of all scopes in the cluster.
 * Then there are per scopes table called _system/_tables/`scope`/streamsInScope-`id`.
 * Each such scope table is protected against recreation of scope by attaching a unique id to the scope when it is created.
 */
public class PravegaTablesScope implements Scope {
    // The scope name which has to be used when creating internally used pravega streams.
    public static final String DELETING_SCOPES_TABLE = "_system/deletingScopes";
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaTablesScope.class));
    private static final String KVTABLES_IN_SCOPE_TABLE_FORMAT = "kvTablesInScope" + SEPARATOR + "%s";
    private static final String READER_GROUPS_IN_SCOPE_TABLE_FORMAT = "readerGroupsInScope" + SEPARATOR + "%s";
    private static final String STREAMS_IN_SCOPE_TABLE_FORMAT = "streamsInScope" + SEPARATOR + "%s";
    private static final String STREAM_TAGS_IN_SCOPE = "streamTagsInScope" + SEPARATOR + "%s" + SEPARATOR + "%s";
    private static final HashHelper HASH = HashHelper.seededWith("StreamTags"); // DO NOT CHANGE THIS STRING.
    private static final int MAX_STREAM_TAG_CHUNKS = 25;
    private static final String LAST_TAG_CHUNK = SEPARATOR + (MAX_STREAM_TAG_CHUNKS - 1);
    private final String scopeName;
    private final PravegaTablesStoreHelper storeHelper;
    private final AtomicReference<UUID> idRef;

    public PravegaTablesScope(final String scopeName, PravegaTablesStoreHelper storeHelper) {
        this.scopeName = scopeName;
        this.storeHelper = storeHelper;
        this.idRef = new AtomicReference<>(null);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        // We will first attempt to create the entry for the scope in scopes table.
        // If scopes table does not exist, we create the scopes table (idempotent)
        // followed by creating a new entry for this scope with a new unique id.
        // We then retrieve id from the store (in case someone concurrently created the entry or entry already existed.
        // This unique id is used to create scope specific table with unique id.
        // If scope entry exists in Scopes table, create the streamsInScope table before throwing DataExists exception
        return Futures.handleCompose(
                        withCreateTableIfAbsent(() -> storeHelper.addNewEntry(
                                SCOPES_TABLE, scopeName, newId(), UUID_TO_BYTES_FUNCTION, context.getRequestId()), SCOPES_TABLE, context), (r, e) -> {
                            if (e == null || Exceptions.unwrap(e) instanceof StoreException.DataExistsException) {
                                return CompletableFuture.allOf(getStreamsInScopeTableName(context)
                                                .thenCompose(streamsTableName -> storeHelper.createTable(streamsTableName, context.getRequestId())
                                                        .thenAccept(v -> {
                                                            log.debug(context.getRequestId(),
                                                                    "table for streams created {}", streamsTableName);
                                                            if (e != null) {
                                                                throw new CompletionException(e);
                                                            }
                                                        })),
                                        getKVTablesInScopeTableName(context)
                                                .thenCompose(kvtsTableName -> storeHelper.createTable(kvtsTableName, context.getRequestId())
                                                        .thenAccept(v -> {
                                                            log.debug(context.getRequestId(), "table for kvts created {}", kvtsTableName);
                                                            if (e != null) {
                                                                throw new CompletionException(e);
                                                            }
                                                        })),
                                        getReaderGroupsInScopeTableName(context)
                                                .thenCompose(rgTableName -> storeHelper.createTable(rgTableName, context.getRequestId())
                                                        .thenAccept(v -> {
                                                            log.debug(context.getRequestId(), "table for reader groups created {}", rgTableName);
                                                            if (e != null) {
                                                                throw new CompletionException(e);
                                                            }
                                                        })),
                                        getAllStreamTagsInScopeTableNames(context)
                                                .thenCompose(tagTableNames -> {
                                                    List<CompletableFuture<Void>> futures = new ArrayList<>();
                                                    for (String tableName : tagTableNames) {
                                                        futures.add(storeHelper.createTable(tableName, context.getRequestId()));
                                                    }
                                                    return Futures.allOf(futures);
                                                })
                                                .thenAccept(v -> {
                                                    if (e != null) {
                                                        throw new CompletionException(e);
                                                    }
                                                }));
                            } else {
                                throw new CompletionException(e);
                            }
                        }).thenCompose(v -> {
                            if (scopeName.equals(INTERNAL_SCOPE_NAME)) {
                                return storeHelper.createTable(DELETING_SCOPES_TABLE, context.getRequestId());
                            }
                            return CompletableFuture.completedFuture(null);
        });
    }

    public CompletableFuture<String> getStreamsInScopeTableName(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getStreamsInScopeTableName(true, context);
    }

    public CompletableFuture<String> getStreamsInScopeTableName(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        if (ignoreCached) {
            storeHelper.invalidateCache(SCOPES_TABLE, scopeName);
        }
        return getId(context).thenApply(id ->
                                                getQualifiedTableName(INTERNAL_SCOPE_NAME, scopeName, String.format(STREAMS_IN_SCOPE_TABLE_FORMAT, id.toString())));
    }

    public CompletableFuture<String> getKVTablesInScopeTableName(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getKVTablesInScopeTableName(true, context);
    }

    public CompletableFuture<String> getKVTablesInScopeTableName(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        if (ignoreCached) {
            storeHelper.invalidateCache(SCOPES_TABLE, scopeName);
        }
        return getId(context).thenApply(id ->
                                                getQualifiedTableName(INTERNAL_SCOPE_NAME, scopeName, String.format(KVTABLES_IN_SCOPE_TABLE_FORMAT, id.toString())));
    }

    public CompletableFuture<String> getAllStreamTagsInScopeTableNames(String stream, OperationContext context) {
        return getId(context)
                .thenApply(id -> getQualifiedTableName(INTERNAL_SCOPE_NAME, scopeName,
                                                       String.format(STREAM_TAGS_IN_SCOPE, id.toString(),
                                                                     HASH.hashToBucket(stream, MAX_STREAM_TAG_CHUNKS))));
    }

    public CompletableFuture<List<String>> getAllStreamTagsInScopeTableNames(OperationContext context) {
        return getId(context).thenApply(id -> IntStream.range(0, MAX_STREAM_TAG_CHUNKS).boxed()
                                                       .map(chunkId -> getQualifiedTableName(INTERNAL_SCOPE_NAME, scopeName, String.format(STREAM_TAGS_IN_SCOPE, id.toString(), chunkId)))
                                                       .collect(Collectors.toList()));
    }

    public CompletableFuture<String> getReaderGroupsInScopeTableName(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getReaderGroupsInScopeTableName(true, context);
    }

    public CompletableFuture<String> getReaderGroupsInScopeTableName(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        if (ignoreCached) {
            storeHelper.invalidateCache(SCOPES_TABLE, scopeName);
        }
        return getId(context).thenApply(id ->
                                                getQualifiedTableName(INTERNAL_SCOPE_NAME, scopeName, String.format(READER_GROUPS_IN_SCOPE_TABLE_FORMAT, id.toString())));
    }

    CompletableFuture<UUID> getId(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        UUID id = idRef.get();
        if (Objects.isNull(id)) {
            return storeHelper.getCachedOrLoad(SCOPES_TABLE, scopeName, BYTES_TO_UUID_FUNCTION, 0L, context.getRequestId())
                              .thenCompose(entry -> {
                                  UUID uuid = entry.getObject();
                                  idRef.compareAndSet(null, uuid);
                                  return getId(context);
                              });
        } else {
            return CompletableFuture.completedFuture(id);
        }
    }

    @Override
    public CompletableFuture<Void> deleteScope(OperationContext context) {
        return deleteMetadataTables(context);
    }

    @Override
    public CompletableFuture<Void> deleteScopeRecursive(OperationContext context) {
        return deleteMetadataTables(context)
                .thenCompose(x -> storeHelper.removeEntry(DELETING_SCOPES_TABLE, scopeName, context.getRequestId()));
    }

    private CompletableFuture<Void> deleteMetadataTables(OperationContext context) {
        log.debug("Processing DeleteMetadatables");
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        CompletableFuture<String> streamsInScopeTableNameFuture = getStreamsInScopeTableName(true, context);
        CompletableFuture<String> rgsInScopeTableNameFuture = getReaderGroupsInScopeTableName(context);
        CompletableFuture<String> kvtsInScopeTableNameFuture = getKVTablesInScopeTableName(context);
        CompletableFuture<List<String>> streamTagsInScopeTableNameFuture = getAllStreamTagsInScopeTableNames(context);
        return CompletableFuture.allOf(streamsInScopeTableNameFuture, rgsInScopeTableNameFuture, kvtsInScopeTableNameFuture, streamTagsInScopeTableNameFuture)
                .thenCompose(x -> {
                    String streamsInScopeTableName = streamsInScopeTableNameFuture.join();
                    String kvtsInScopeTableName = kvtsInScopeTableNameFuture.join();
                    String rgsInScopeTableName = rgsInScopeTableNameFuture.join();
                    List<String> streamTagsInScopeTableNames = streamTagsInScopeTableNameFuture.join();
                    val deleteTagTablesFut = streamTagsInScopeTableNames.stream()
                            .map(tableName -> storeHelper.deleteTable(tableName, false, context.getRequestId()))
                            .collect(Collectors.toList());
                    return CompletableFuture.allOf(
                                    storeHelper.deleteTable(streamsInScopeTableName, true, context.getRequestId()),
                                    storeHelper.deleteTable(kvtsInScopeTableName, true, context.getRequestId()),
                                    storeHelper.deleteTable(rgsInScopeTableName, true, context.getRequestId()),
                                    Futures.allOf(deleteTagTablesFut))
                            .thenAccept(v -> log.debug(context.getRequestId(), "tables deleted {} {} {}", streamsInScopeTableName,
                                    kvtsInScopeTableName, rgsInScopeTableName));
                })
                .thenCompose(deleted -> storeHelper.removeEntry(SCOPES_TABLE, scopeName, context.getRequestId()));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreams(int limit, String continuationToken, Executor executor,
                                                                     OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getStreamsInScopeTableName(context)
                .thenCompose(streamsInScopeTable -> readAll(limit, continuationToken, streamsInScopeTable, context));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreamsForTag(String tag, String continuationToken, Executor executor,
                                                                           OperationContext context) {

        Preconditions.checkNotNull(context, "Operation context cannot be null");

        return getStreamsFromNextTagChunk(tag, continuationToken, context).thenCompose(pair -> {
            if (pair.getLeft().isEmpty() && !pair.getRight().endsWith(LAST_TAG_CHUNK)) {
                return listStreamsForTag(tag, pair.getRight(), executor, context);
            } else {
                return CompletableFuture.completedFuture(pair);
            }
        });
    }

    /**
     * Fetch the Streams from the next available Tag Chunk table.
     *
     * @param tag Stream Tag
     * @param token The token corresponds to the last Tag Chunk table that data was read from.
     * @param context Operation context.
     * @return A future that returns a List of Streams and the token.
     */
    CompletableFuture<Pair<List<String>, String>> getStreamsFromNextTagChunk(String tag, String token, OperationContext context) {
        if (token.endsWith(LAST_TAG_CHUNK)) {
            return CompletableFuture.completedFuture(new ImmutablePair<>(Collections.emptyList(), token));
        } else {
            return getAllStreamTagsInScopeTableNames(context).thenApply(
                    chunkTableList -> {
                        if (token.isEmpty()) {
                            // token is empty, try reading from the first tag table.
                            return chunkTableList.get(0);
                        } else {
                            // return next index
                            return chunkTableList.get(chunkTableList.indexOf(token) + 1);
                        }
                    }
            ).thenCompose(table -> storeHelper.expectingDataNotFound(
                    storeHelper.getEntry(table, tag, TagRecord::fromBytes, context.getRequestId())
                               .thenApply(ver -> new ImmutablePair<>(new ArrayList<>(ver.getObject().getStreams()), table)),
                    new ImmutablePair<>(Collections.emptyList(), table)));
        }
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        List<String> result = new ArrayList<>();
        return getStreamsInScopeTableName(context)
                .thenCompose(tableName -> Futures.exceptionallyExpecting(storeHelper.getAllKeys(tableName,
                                                                                                context.getRequestId()).collectRemaining(result::add)
                                                                                    .thenApply(v -> result), DATA_NOT_FOUND_PREDICATE, Collections.emptyList()));
    }

    @Override
    public void refresh() {
        idRef.set(null);
    }

    public CompletableFuture<Void> addStreamToScope(String stream, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getStreamsInScopeTableName(context)
                .thenCompose(tableName -> Futures.toVoid(
                        withCreateTableIfAbsent(() -> storeHelper.addNewEntryIfAbsent(tableName, stream, newId(),
                                                                                      UUID_TO_BYTES_FUNCTION, context.getRequestId()),
                                                tableName, context)));
    }

    public CompletableFuture<Void> sealScope(String scope, OperationContext context) {
        return getId(context).thenCompose(id -> Futures.toVoid(storeHelper.addNewEntry(DELETING_SCOPES_TABLE, scope, id, UUID_TO_BYTES_FUNCTION, context.getRequestId())));
    }

    /**
     * Associate the Stream with corresponding Tags.
     * @param stream Stream to be associated with the tags.
     * @param tags Stream Tags.
     * @param context Context.
     * @return A future which completes once the update is complete.
     */
    public CompletableFuture<Void> addTagsUnderScope(String stream, Set<String> tags, OperationContext context) {
        return getAllStreamTagsInScopeTableNames(stream, context)
                .thenCompose(table -> Futures.allOf(tags.stream()
                                                    .parallel()
                                                    .map(key -> {
                                             log.debug(context.getRequestId(), "Adding stream {} to tag {} index on table {}", stream, key, table);
                                             return storeHelper.getAndUpdateEntry(table, key,
                                                                                  e -> appendStreamToEntry(key, stream, e),
                                                                                  e -> false, // no need of cleanup
                                                                                  context.getRequestId());
                                         })
                                                    .collect(Collectors.toList())));
    }

    private TableSegmentEntry appendStreamToEntry(String tag, String appendValue, TableSegmentEntry entry) {
        byte[] array = storeHelper.getArray(entry.getValue());
        byte[] updatedBytes;
        if (array.length == 0) {
            updatedBytes = TagRecord.builder().tagName(tag).stream(appendValue).build().toBytes();
        } else {
            TagRecord record = TagRecord.fromBytes(array);
            updatedBytes = record.toBuilder().stream(appendValue).build().toBytes();
        }
        return TableSegmentEntry.versioned(tag.getBytes(StandardCharsets.UTF_8), updatedBytes,
                                           entry.getKey().getVersion().getSegmentVersion());
    }

    /**
     * Remove Tags for the specified Stream.
     * @param stream Stream for which tags are removed.
     * @param tags Stream tags.
     * @param context Context.
     * @return A future which completes once the update is complete.
     */
    public CompletableFuture<Void> removeTagsUnderScope(String stream, Set<String> tags, OperationContext context) {
        return getAllStreamTagsInScopeTableNames(stream, context)
                .thenCompose(table -> Futures.allOf(tags.stream()
                                                        .parallel()
                                                        .map(key -> {
                                                            log.debug(context.getRequestId(), "Removing stream {} from tag {} index on table {}", stream, key, table);
                                                            return storeHelper.getAndUpdateEntry(table, key,
                                                                                                 e -> removeStreamFromEntry(key, stream, e),
                                                                                                 this::isEmptyTagRecord,
                                                                                                 context.getRequestId());
                                                        })
                                                        .collect(Collectors.toList())));
    }

    private TableSegmentEntry removeStreamFromEntry(String tag, String removeValue, TableSegmentEntry currentEntry) {
        byte[] array = storeHelper.getArray(currentEntry.getValue());
        byte[] updatedBytes = new byte[0];
        if (array.length != 0) {
            TagRecord record = TagRecord.fromBytes(array);
            //Remove Stream from TagRecord.
            TagRecord updatedRecord = record.toBuilder().removeStream(removeValue).build();
            updatedBytes = updatedRecord.toBytes();
        }
        return TableSegmentEntry.versioned(tag.getBytes(StandardCharsets.UTF_8),
                                           updatedBytes,
                                           currentEntry.getKey().getVersion().getSegmentVersion());
    }

    private boolean isEmptyTagRecord(TableSegmentEntry entry) {
        byte[] array = storeHelper.getArray(entry.getValue());
        return array.length == 0 || TagRecord.fromBytes(array).getStreams().isEmpty();
    }

    public CompletableFuture<Void> removeStreamFromScope(String stream, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getStreamsInScopeTableName(context)
                .thenCompose(tableName -> Futures.toVoid(storeHelper.removeEntry(tableName, stream, context.getRequestId())));
    }

    public CompletableFuture<Boolean> checkStreamExistsInScope(String stream, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getStreamsInScopeTableName(context)
                .thenCompose(tableName -> storeHelper.expectingDataNotFound(
                        storeHelper.getEntry(tableName, stream, x -> x, context.getRequestId()).thenApply(v -> true),
                        false));
    }

    @Override
    public CompletableFuture<Boolean> isScopeSealed(String scopeName, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return storeHelper.expectingDataNotFound(
                        storeHelper.getEntry(DELETING_SCOPES_TABLE, scopeName, x -> x, context.getRequestId()).thenApply(v -> true),
                        false);
    }

    public CompletableFuture<Boolean> checkKeyValueTableExistsInScope(String kvt, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getKVTablesInScopeTableName(context)
                .thenCompose(tableName -> storeHelper.expectingDataNotFound(
                        storeHelper.getEntry(tableName, kvt, x -> x, context.getRequestId()).thenApply(v -> true),
                        false));
    }

    public CompletableFuture<Boolean> checkReaderGroupExistsInScope(String readerGroupName, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getReaderGroupsInScopeTableName(context)
                .thenCompose(tableName -> storeHelper.expectingDataNotFound(
                        storeHelper.getEntry(tableName, readerGroupName, x -> x, context.getRequestId()).thenApply(v -> true),
                        false));
    }

    public CompletableFuture<Void> addKVTableToScope(String kvt, UUID id, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getKVTablesInScopeTableName(context)
                .thenCompose(tableName -> Futures.toVoid(
                        withCreateTableIfAbsent(() -> storeHelper.addNewEntryIfAbsent(tableName, kvt, id,
                                                                                      UUID_TO_BYTES_FUNCTION, context.getRequestId()), tableName, context)));
    }

    public CompletableFuture<Void> removeKVTableFromScope(String kvt, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getKVTablesInScopeTableName(context)
                .thenCompose(tableName -> Futures.toVoid(storeHelper.removeEntry(tableName, kvt, context.getRequestId())));
    }

    public CompletableFuture<Void> addReaderGroupToScope(String readerGroupName, UUID readerGroupId, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getReaderGroupsInScopeTableName(context)
                .thenCompose(tableName -> Futures.toVoid(withCreateTableIfAbsent(
                        () -> storeHelper.addNewEntryIfAbsent(tableName, readerGroupName, readerGroupId,
                                                              UUID_TO_BYTES_FUNCTION, context.getRequestId()),
                        tableName, context)));
    }

    public CompletableFuture<Void> removeReaderGroupFromScope(String readerGroup, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getReaderGroupsInScopeTableName(context)
                .thenCompose(tableName -> Futures.toVoid(storeHelper.removeEntry(tableName, readerGroup, context.getRequestId())));
    }

    @Override
    public CompletableFuture<UUID> getReaderGroupId(String readerGroupName, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getReaderGroupsInScopeTableName(context)
                .thenCompose(tableName -> storeHelper.getEntry(tableName, readerGroupName, BYTES_TO_UUID_FUNCTION,
                                                               context.getRequestId())
                                                     .thenApply(VersionedMetadata::getObject));
    }

    @Override
    public CompletableFuture<UUID> getScopeId(String scopeName, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getId(context);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listKeyValueTables(int limit, String continuationToken,
                                                                            Executor executor, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getKVTablesInScopeTableName(context)
                .thenCompose(kvtablesInScopeTable -> readAll(limit, continuationToken, kvtablesInScopeTable, context));
    }

    private <T> CompletableFuture<T> withCreateTableIfAbsent(Supplier<CompletableFuture<T>> futureSupplier, String tableName,
                                                             OperationContext context) {
        return Futures.exceptionallyComposeExpecting(futureSupplier.get(),
                                                     DATA_NOT_FOUND_PREDICATE, () -> storeHelper.createTable(tableName, context.getRequestId()).thenCompose(v -> futureSupplier.get()));
    }

    private CompletableFuture<Pair<List<String>, String>> readAll(int limit, String continuationToken, String tableName,
                                                                  OperationContext context) {
        List<String> taken = new ArrayList<>();
        AtomicReference<String> token = new AtomicReference<>(continuationToken);
        AtomicBoolean canContinue = new AtomicBoolean(true);

        return Futures.exceptionallyExpecting(storeHelper.getKeysPaginated(tableName,
                                                                           Unpooled.wrappedBuffer(Base64.getDecoder().decode(token.get())), limit, context.getRequestId())
                                                         .thenApply(result -> {
                                                             if (result.getValue().isEmpty()) {
                                                                 canContinue.set(false);
                                                             } else {
                                                                 taken.addAll(result.getValue());
                                                             }
                                                             token.set(Base64.getEncoder().encodeToString(result.getKey().array()));
                                                             return new ImmutablePair<>(taken, token.get());
                                                         }), DATA_NOT_FOUND_PREDICATE, new ImmutablePair<>(Collections.emptyList(), token.get()));
    }
}
