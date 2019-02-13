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
import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTag;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StoreException;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Zookeeper based host index.
 */
@Slf4j
public class PravegaTablesHostIndex implements HostIndex {
    public static final String SYSTEM = "_system";
    private final PravegaTablesStoreHelper storeHelper;
    private final Executor executor;
    private final String hostRoot;
    private final Map<String, Boolean> hostIdMap;
    
    public PravegaTablesHostIndex(SegmentHelper segmentHelper, String hostRoot, Executor executor) {
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper);
        this.executor = executor;
        this.hostRoot = hostRoot;
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
        String table = getTable(hostId);

        // 1. create table with name hostId
        CompletableFuture<Boolean> createTableFuture; 
        if (Optional.of(hostIdMap.get(hostId)).orElse(false)) {
            createTableFuture = segmentHelper.createTableSegment()
            segmentHelper.createTableSegment(SYSTEM, table, RequestTag.NON_EXISTENT_ID);
        } else {
            createTableFuture = CompletableFuture.completedFuture(true);
        }
        
        return createTableFuture.thenCompose(x -> {
            // TODO: shivesh:: 
            TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(entity.getBytes(), KeyVersion.NOT_EXISTS), entityData);
            return Futures.toVoid(segmentHelper.createTableEntryIfAbsent(SYSTEM, table, entry, RequestTag.NON_EXISTENT_ID));
        });
    }

    private String getTable(String hostId) {
        return hostRoot + hostId;
    }

    @Override
    public CompletableFuture<byte[]> getEntityData(String hostId, String entity) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        String table = getTable(hostId);
        return segmentHelper.readKey(SYSTEM, table, entity.getBytes(), RequestTag.NON_EXISTENT_ID)
                .thenApply(TableEntry::getValue);
    }

    @Override
    public CompletableFuture<Void> removeEntity(final String hostId, final String entity, final boolean deleteEmptyHost) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        String table = getTable(hostId);
        return segmentHelper.removeTableKey(SYSTEM, table, entity.getBytes(), RequestTag.NON_EXISTENT_ID)
                .thenCompose(e -> {
                    if (deleteEmptyHost) {
                        return Futures.toVoid(segmentHelper.deleteTableSegment(SYSTEM, table, true, RequestTag.NON_EXISTENT_ID));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> removeHost(final String hostId) {
        Preconditions.checkNotNull(hostId);
        String table = getTable(hostId);
        return segmentHelper.deleteTableSegment(SYSTEM, table, true, RequestTag.NON_EXISTENT_ID)
                .thenCompose(deleted -> {
                    if (deleted) {
                        return segmentHelper.removeTableKey(SYSTEM, hostsTable, )     
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> getEntities(final String hostId) {
        Preconditions.checkNotNull(hostId);
        // TODO: shivesh
        return getChildren(getHostPath(hostId));
    }

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        return getChildren(hostRoot).thenApply(list -> list.stream().collect(Collectors.toSet()));
    }
}
