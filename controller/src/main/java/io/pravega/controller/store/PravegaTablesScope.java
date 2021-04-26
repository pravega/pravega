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

import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.StoreException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SCOPES_TABLE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SEPARATOR;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * Pravega Tables based scope metadata.
 * At top level, there is a common scopes table at _system. This has a list of all scopes in the cluster.
 * Then there are per scopes table called _system/_tables/`scope`/streamsInScope-`id`.
 * Each such scope table is protected against recreation of scope by attaching a unique id to the scope when it is created.
 */
@Slf4j
public class PravegaTablesScope implements Scope {
    private static final String STREAMS_IN_SCOPE_TABLE_FORMAT = "streamsInScope" + SEPARATOR + "%s";
    private static final String KVTABLES_IN_SCOPE_TABLE_FORMAT = "kvTablesInScope" + SEPARATOR + "%s";
    private static final String READER_GROUPS_IN_SCOPE_TABLE_FORMAT = "readerGroupsInScope" + SEPARATOR + "%s";
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
    public CompletableFuture<Void> createScope() {
        // We will first attempt to create the entry for the scope in scopes table.
        // If scopes table does not exist, we create the scopes table (idempotent)
        // followed by creating a new entry for this scope with a new unique id.
        // We then retrive id from the store (in case someone concurrently created the entry or entry already existed.
        // This unique id is used to create scope specific table with unique id.
        // If scope entry exists in Scopes table, create the streamsInScope table before throwing DataExists exception
        return Futures.handleCompose(withCreateTableIfAbsent(() -> storeHelper.addNewEntry(
                SCOPES_TABLE, scopeName, newId()), SCOPES_TABLE), (r, e) -> {
            if (e == null || Exceptions.unwrap(e) instanceof StoreException.DataExistsException) {
                return CompletableFuture.allOf(getStreamsInScopeTableName()
                        .thenCompose(streamsTableName -> storeHelper.createTable(streamsTableName)
                                                             .thenAccept(v -> {
                                                                 log.debug("table for streams created {}", streamsTableName);
                                                                 if (e != null) {
                                                                     throw new CompletionException(e);
                                                                 }
                                                             })),
                        getKVTablesInScopeTableName()
                        .thenCompose(kvtsTableName -> storeHelper.createTable(kvtsTableName)
                                .thenAccept(v -> {
                                    log.debug("table for kvts created {}", kvtsTableName);
                                    if (e != null) {
                                        throw new CompletionException(e);
                                    }
                                })),
                        getReaderGroupsInScopeTableName()
                        .thenCompose(rgTableName -> storeHelper.createTable(rgTableName)
                                .thenAccept(v -> {
                                    log.debug("table for reader groups created {}", rgTableName);
                                    if (e != null) {
                                          throw new CompletionException(e);
                                    }
                                }))
                        );
            } else {
                throw new CompletionException(e);
            }
        });
    }

    public CompletableFuture<String> getStreamsInScopeTableName() {
        return getId().thenApply(id ->
                getQualifiedTableName(INTERNAL_SCOPE_NAME, scopeName, String.format(STREAMS_IN_SCOPE_TABLE_FORMAT, id.toString())));
    }

    public CompletableFuture<String> getKVTablesInScopeTableName() {
        return getId().thenApply(id ->
                getQualifiedTableName(INTERNAL_SCOPE_NAME, scopeName, String.format(KVTABLES_IN_SCOPE_TABLE_FORMAT, id.toString())));
    }

    public CompletableFuture<String> getReaderGroupsInScopeTableName() {
        return getId().thenApply(id ->
                getQualifiedTableName(INTERNAL_SCOPE_NAME, scopeName, String.format(READER_GROUPS_IN_SCOPE_TABLE_FORMAT, id.toString())));
    }

    CompletableFuture<UUID> getId() {
        UUID id = idRef.get();
        if (Objects.isNull(id)) {
            return storeHelper.getEntry(SCOPES_TABLE, scopeName, x -> BitConverter.readUUID(x, 0))
                              .thenCompose(entry -> {
                                  UUID uuid = entry.getObject();
                                  idRef.compareAndSet(null, uuid);
                                  return getId();
                              });
        } else {
            return CompletableFuture.completedFuture(id);
        }
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        CompletableFuture<String> streamsInScopeTableNameFuture = getStreamsInScopeTableName();
        CompletableFuture<String> rgsInScopeTableNameFuture = getReaderGroupsInScopeTableName();
        CompletableFuture<String> kvtsInScopeTableNameFuture = getKVTablesInScopeTableName();
        return CompletableFuture.allOf(streamsInScopeTableNameFuture, rgsInScopeTableNameFuture, kvtsInScopeTableNameFuture)
                .thenCompose(x -> {
                    String streamsInScopeTableName = streamsInScopeTableNameFuture.join();
                    String kvtsInScopeTableName = kvtsInScopeTableNameFuture.join();
                    String rgsInScopeTableName = rgsInScopeTableNameFuture.join();
                    return CompletableFuture.allOf(storeHelper.deleteTable(streamsInScopeTableName, true),
                            storeHelper.deleteTable(kvtsInScopeTableName, true), 
                            storeHelper.deleteTable(rgsInScopeTableName, true))
                                     .thenAccept(v -> log.debug("tables deleted {} {} {}", streamsInScopeTableName,
                                             kvtsInScopeTableName, rgsInScopeTableName));
                })
                .thenCompose(deleted -> storeHelper.removeEntry(SCOPES_TABLE, scopeName));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreams(int limit, String continuationToken, Executor executor) {
        return getStreamsInScopeTableName()
                .thenCompose(streamsInScopeTable -> readAll(limit, continuationToken, streamsInScopeTable));
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope() {
        List<String> result = new ArrayList<>();
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> Futures.exceptionallyExpecting(storeHelper.getAllKeys(tableName).collectRemaining(result::add)
                                                     .thenApply(v -> result), DATA_NOT_FOUND_PREDICATE, Collections.emptyList()));
    }

    @Override
    public void refresh() {
        idRef.set(null);
    }

    public CompletableFuture<Void> addStreamToScope(String stream) {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(
                        withCreateTableIfAbsent(() -> storeHelper.addNewEntryIfAbsent(tableName, stream, newId()), 
                        tableName)));
    }

    public CompletableFuture<Void> removeStreamFromScope(String stream) {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(storeHelper.removeEntry(tableName, stream)));
    }

    public CompletableFuture<Boolean> checkStreamExistsInScope(String stream) {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> storeHelper.expectingDataNotFound(
                        storeHelper.getEntry(tableName, stream, x -> x).thenApply(v -> true), false));
    }

    public CompletableFuture<Boolean> checkKeyValueTableExistsInScope(String kvt) {
        return getKVTablesInScopeTableName()
                .thenCompose(tableName -> storeHelper.expectingDataNotFound(
                        storeHelper.getEntry(tableName, kvt, x -> x).thenApply(v -> true), false));
    }

    public CompletableFuture<Boolean> checkReaderGroupExistsInScope(String readerGroupName) {
        return getReaderGroupsInScopeTableName()
                .thenCompose(tableName -> storeHelper.expectingDataNotFound(
                        storeHelper.getEntry(tableName, readerGroupName, x -> x).thenApply(v -> true), false));
    }

    public CompletableFuture<Void> addKVTableToScope(String kvt, byte[] id) {
        return getKVTablesInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(
                        withCreateTableIfAbsent(() -> storeHelper.addNewEntryIfAbsent(tableName, kvt, id), tableName)));
    }

    public CompletableFuture<Void> removeKVTableFromScope(String kvt) {
        return getKVTablesInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(storeHelper.removeEntry(tableName, kvt)));
    }

    public CompletableFuture<Void> addReaderGroupToScope(String readerGroupName, UUID readerGroupId) {
        return getReaderGroupsInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(withCreateTableIfAbsent(
                        () -> storeHelper.addNewEntryIfAbsent(tableName, readerGroupName, getIdInBytes(readerGroupId)),
                        tableName)));
    }

    public CompletableFuture<Void> removeReaderGroupFromScope(String readerGroup) {
        return getReaderGroupsInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(storeHelper.removeEntry(tableName, readerGroup)));
    }

    public CompletableFuture<UUID> getReaderGroupId(String readerGroupName) {
        return getReaderGroupsInScopeTableName()
                .thenCompose(tableName -> storeHelper.getEntry(tableName, readerGroupName, id -> BitConverter.readUUID(id, 0))
                        .thenApply(versionedUUID -> versionedUUID.getObject()));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listKeyValueTables(int limit, String continuationToken,
                                                                            Executor executor) {
        return getKVTablesInScopeTableName()
                .thenCompose(kvtablesInScopeTable -> readAll(limit, continuationToken, kvtablesInScopeTable));
    }

    private <T> CompletableFuture<T> withCreateTableIfAbsent(Supplier<CompletableFuture<T>> futureSupplier, String tableName) {
        return Futures.exceptionallyComposeExpecting(futureSupplier.get(), 
                DATA_NOT_FOUND_PREDICATE, () -> storeHelper.createTable(tableName).thenCompose(v -> futureSupplier.get()));
    } 
    
    private CompletableFuture<Pair<List<String>, String>> readAll(int limit, String continuationToken, String tableName) {
        List<String> taken = new ArrayList<>();
        AtomicReference<String> token = new AtomicReference<>(continuationToken);
        AtomicBoolean canContinue = new AtomicBoolean(true);

        return Futures.exceptionallyExpecting(storeHelper.getKeysPaginated(tableName,
                Unpooled.wrappedBuffer(Base64.getDecoder().decode(token.get())), limit)
                          .thenApply(result -> {
                              if (result.getValue().isEmpty()) {
                                  canContinue.set(false);
                              } else {
                                  taken.addAll(result.getValue());
                              }
                              token.set(Base64.getEncoder().encodeToString(result.getKey().array()));
                              return new ImmutablePair<>(taken, token.get());
                          }), DATA_NOT_FOUND_PREDICATE, new ImmutablePair<>(Collections.emptyList(), null));
    }
}
