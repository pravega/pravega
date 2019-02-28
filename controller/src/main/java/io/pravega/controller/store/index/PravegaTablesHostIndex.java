/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.index;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.Data;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.Version;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_EMPTY_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SEPARATOR;

/**
 * Pravega Tables based host index.
 * The tables are structured as follows:
 * Hosts table which has list of all hosts.
 * `host`-`index`-entity table which is a dedicated table per host for storing a specific type of entity. 
 */
@Slf4j
public class PravegaTablesHostIndex implements HostIndex {
    private static final String HOSTS_ROOT_TABLE_FORMAT = "Table" + SEPARATOR + "hosts" + SEPARATOR + "%s";
    private static final String HOST_TABLE_FORMAT = "Table" + SEPARATOR + "host" + SEPARATOR + "%s" + SEPARATOR + "%s";
    private final PravegaTablesStoreHelper storeHelper;
    private final String hostsTable;
    private final String indexName;

    public PravegaTablesHostIndex(String indexName, PravegaTablesStoreHelper storeHelper) {
        this.storeHelper = storeHelper;
        this.indexName = indexName;
        this.hostsTable = String.format(HOSTS_ROOT_TABLE_FORMAT, this.indexName);
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

        return Futures.toVoid(Futures.exceptionallyComposeExpecting(
                storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, hostEntityTable, entity, entityData),
                DATA_NOT_FOUND_PREDICATE,
                () -> handleTableNotExist(hostId, entity, entityData, hostEntityTable)));
    }

    private CompletableFuture<Version> handleTableNotExist(String hostId, String entity, byte[] entityData, String hostEntityTable) {
        // This method is called if hostEntity table does not exist.
        // It will first create new hosts table (idempotent) and add new entry for this host (if absent)
        // Then it will create hostEntity table and add the entry to it. 
        // CreateTable is idempotent and returns success if table exists, creates it otherwise. 
        // addNewEntryIfAbsent ignores DataExists hence it is idempotent. 
        // This method can be called concurrently across different controller instances and since each step is independently
        // idempotent, the overall flow is idempotent and does not fail if attempted again. 
        return storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, hostsTable)
                          .thenAccept(x -> log.debug("created hosts root table {}", hostId))
                          .thenCompose(x -> storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, hostsTable, hostId, new byte[0]))
                          .thenCompose(x -> storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, hostEntityTable)
                                                       .thenAccept(v -> log.debug("creating table {} for host {}", hostEntityTable, hostId)))
                          .thenCompose(x -> storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, hostEntityTable, entity, entityData));
    }

    private String getHostEntityTableName(String hostId) {
        return String.format(HOST_TABLE_FORMAT, indexName, hostId);
    }

    @Override
    public CompletableFuture<byte[]> getEntityData(String hostId, String entity) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        String table = getHostEntityTableName(hostId);
        return Futures.exceptionallyExpecting(storeHelper.getEntry(NameUtils.INTERNAL_SCOPE_NAME, table, entity)
                                                         .thenApply(Data::getData), DATA_NOT_FOUND_PREDICATE, null);
    }

    @Override
    public CompletableFuture<Void> removeEntity(final String hostId, final String entity, final boolean deleteEmptyHost) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        String table = getHostEntityTableName(hostId);
        return Futures.exceptionallyExpecting(storeHelper.removeEntry(NameUtils.INTERNAL_SCOPE_NAME, table, entity),
                DATA_NOT_FOUND_PREDICATE, null);
    }

    @Override
    public CompletableFuture<Void> removeHost(final String hostId) {
        Preconditions.checkNotNull(hostId);
        String table = getHostEntityTableName(hostId);
        return Futures.exceptionallyExpecting(Futures.exceptionallyExpecting(
                storeHelper.deleteTable(NameUtils.INTERNAL_SCOPE_NAME, table, true)
                           .thenApply(v -> {
                               log.debug("deleted table {}", table);
                               return true;
                           }),
                DATA_NOT_EMPTY_PREDICATE, false), DATA_NOT_FOUND_PREDICATE, true)
                      .thenCompose(deleted -> {
                          if (deleted) {
                              return Futures.exceptionallyExpecting(storeHelper.removeEntry(NameUtils.INTERNAL_SCOPE_NAME, hostsTable, hostId),
                                      DATA_NOT_FOUND_PREDICATE, null);
                          } else {
                              return CompletableFuture.completedFuture(null);
                          }
                      });
    }

    @Override
    public CompletableFuture<List<String>> getEntities(final String hostId) {
        Preconditions.checkNotNull(hostId);
        String table = getHostEntityTableName(hostId);
        List<String> entries = new ArrayList<>();
        return Futures.exceptionallyExpecting(storeHelper.getAllKeys(NameUtils.INTERNAL_SCOPE_NAME, table)
                                                         .collectRemaining(entries::add)
                                                         .thenApply(x -> entries), DATA_NOT_FOUND_PREDICATE, Collections.emptyList());
    }

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        Set<String> hosts = new ConcurrentSkipListSet<>();
        return Futures.exceptionallyExpecting(storeHelper.getAllKeys(NameUtils.INTERNAL_SCOPE_NAME, hostsTable)
                                                         .collectRemaining(hosts::add)
                                                         .thenApply(v -> hosts), DATA_NOT_FOUND_PREDICATE, Collections.emptySet());
    }
}
