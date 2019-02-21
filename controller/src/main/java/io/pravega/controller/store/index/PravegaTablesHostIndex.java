/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.store.index;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.stream.Data;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StoreException;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

/**
 * Zookeeper based host index.
 */
@Slf4j
public class PravegaTablesHostIndex implements HostIndex {
    private static final String SYSTEM_SCOPE = "_system";
    private static final String HOSTS_ROOT_TABLE_FORMAT = "hostsTable-%s";
    private static final String HOST_TABLE_FORMAT = "host-%s-%s";
    public static final Predicate<Throwable> DATA_NOT_FOUND_PREDICATE = e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException;
    public static final Predicate<Throwable> DATA_NOT_EMPTY_PREDICATE = ex -> Exceptions.unwrap(ex) instanceof StoreException.DataNotEmptyException;
    private final PravegaTablesStoreHelper storeHelper;
    private final Executor executor;
    private final String hostsTable;
    private final String indexName;
    private final ConcurrentHashMap<String, Boolean> hostIdMap;
    
    public PravegaTablesHostIndex(SegmentHelper segmentHelper, String indexName, Executor executor) {
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper, executor);
        this.executor = executor;
        this.indexName = indexName;
        this.hostsTable = String.format(HOSTS_ROOT_TABLE_FORMAT, this.indexName);
        hostIdMap = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<Void> addEntity(final String hostId, final String entity) {
        return addEntity(hostId, entity, new byte[0]);
    }

    @Override
    public CompletableFuture<Void> addEntity(String hostId, String entity, byte[] entityData) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        String hostEntityTable = getHostEntityTableName(hostId);

        // 1. Add host to hosts table first. create table with name hostId
        CompletableFuture<Void> createTableFuture; 
        if (!Optional.ofNullable(hostIdMap.get(hostId)).orElse(false)) {
            createTableFuture = storeHelper.createTable(SYSTEM_SCOPE, hostsTable)
                    .thenCompose(x -> storeHelper.addNewEntryIfAbsent(SYSTEM_SCOPE, hostsTable, hostId, new byte[0]))
                    .thenCompose(x -> storeHelper.createTable(SYSTEM_SCOPE, hostEntityTable))
                                           
                    .thenAccept(x -> hostIdMap.put(hostId, true));
        } else {
            createTableFuture = CompletableFuture.completedFuture(null);
        }
        
        return createTableFuture.thenCompose(x -> {
            return Futures.toVoid(storeHelper.addNewEntryIfAbsent(SYSTEM_SCOPE, hostEntityTable, entity, entityData));
        });
    }

    private String getHostEntityTableName(String hostId) {
        return String.format(HOST_TABLE_FORMAT, indexName, hostId);
    }

    @Override
    public CompletableFuture<byte[]> getEntityData(String hostId, String entity) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        String table = getHostEntityTableName(hostId);
        return Futures.exceptionallyExpecting(storeHelper.getEntry(SYSTEM_SCOPE, table, entity)
                .thenApply(Data::getData), DATA_NOT_FOUND_PREDICATE, null);
    }

    @Override
    public CompletableFuture<Void> removeEntity(final String hostId, final String entity, final boolean deleteEmptyHost) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        String table = getHostEntityTableName(hostId);
        return Futures.exceptionallyExpecting(storeHelper.removeEntry(SYSTEM_SCOPE, table, entity),
                DATA_NOT_FOUND_PREDICATE, null)
                .thenCompose(v -> {
                    if (deleteEmptyHost) {
                        return Futures.exceptionallyExpecting(removeHost(hostId),
                                DATA_NOT_EMPTY_PREDICATE, null);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> removeHost(final String hostId) {
        Preconditions.checkNotNull(hostId);
        String table = getHostEntityTableName(hostId);
        return Futures.exceptionallyExpecting(storeHelper.deleteTable(SYSTEM_SCOPE, table, true), DATA_NOT_FOUND_PREDICATE, true)
                .thenCompose(deleted -> {
                    if (deleted) {
                        return Futures.exceptionallyExpecting(storeHelper.removeEntry(SYSTEM_SCOPE, hostsTable, hostId), DATA_NOT_EMPTY_PREDICATE, null);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> getEntities(final String hostId) {
        Preconditions.checkNotNull(hostId);
        String table = getHostEntityTableName(hostId);
        List<String> entries = new LinkedList<>();
        return storeHelper.getAllKeys(SYSTEM_SCOPE, table)
                .forEachRemaining(entries::add, executor)
                .thenApply(x -> entries);
    }

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        Set<String> hosts = new ConcurrentSkipListSet<>();
        return storeHelper.getAllKeys(SYSTEM_SCOPE, hostsTable)
                          .forEachRemaining(hosts::add, executor)
                          .thenApply(v -> hosts);
    }
}
