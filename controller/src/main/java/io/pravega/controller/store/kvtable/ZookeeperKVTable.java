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
package io.pravega.controller.store.kvtable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.ZKScope;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.kvtable.records.KVTConfigurationRecord;
import io.pravega.controller.store.kvtable.records.KVTEpochRecord;
import io.pravega.controller.store.kvtable.records.KVTStateRecord;
import io.pravega.controller.store.stream.OperationContext;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ZK KVTable. It understands the following.
 * 1. underlying file organization/object structure of KVTable metadata store.
 * 2. how to evaluate basic read and update queries defined in the KVTable interface.
 * <p>
 * It may cache files read from the store for its lifetime.
 * This shall reduce store round trips for answering queries, thus making them efficient.
 */
@Slf4j
class ZookeeperKVTable extends AbstractKVTableBase {
    private static final String SCOPE_PATH = "/store/%s";
    private static final String KVTABLE_PATH = SCOPE_PATH + "/kvt/%s";
    private static final String CREATION_TIME_PATH = KVTABLE_PATH + "/creationTime";
    private static final String CONFIGURATION_PATH = KVTABLE_PATH + "/configuration";
    private static final String STATE_PATH = KVTABLE_PATH + "/state";
    private static final String CURRENT_EPOCH_RECORD = KVTABLE_PATH + "/currentEpochRecord";
    private static final String EPOCH_RECORD = KVTABLE_PATH + "/epochRecords";
    private static final String ID_PATH = KVTABLE_PATH + "/id";

    private final ZKStoreHelper zkStoreHelper;
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final String creationPath;
    private final String configurationPath;
    private final String statePath;
    private final String idPath;
    @Getter(AccessLevel.PACKAGE)
    private final String kvtablePath;

    private final String currentEpochRecordPath;
    private final String epochRecordPathFormat;
    private final Executor executor;
    private final AtomicReference<String> idRef;

    @VisibleForTesting
    ZookeeperKVTable(final String scopeName, final String kvtName, ZKStoreHelper storeHelper, Executor executor) {
        super(scopeName, kvtName);
        zkStoreHelper = storeHelper;
        kvtablePath = String.format(KVTABLE_PATH, scopeName, kvtName);
        creationPath = String.format(CREATION_TIME_PATH, scopeName, kvtName);
        configurationPath = String.format(CONFIGURATION_PATH, scopeName, kvtName);
        statePath = String.format(STATE_PATH, scopeName, kvtName);
        idPath = String.format(ID_PATH, scopeName, kvtName);
        currentEpochRecordPath = String.format(CURRENT_EPOCH_RECORD, scopeName, kvtName);
        epochRecordPathFormat = String.format(EPOCH_RECORD, scopeName, kvtName) + "/%d";
        idRef = new AtomicReference<>();
        this.executor = executor;
    }

    // region overrides
    @Override
    public CompletableFuture<CreateKVTableResponse> checkKeyValueTableExists(final KeyValueTableConfiguration configuration,
                                                                             final long creationTime, final int startingSegmentNumber, 
                                                                             OperationContext context) {
        // If stream exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run. If the existing stream has already been created successfully earlier,
        return zkStoreHelper.checkExists(creationPath).thenCompose(exists -> {
            if (!exists) {
                return CompletableFuture.completedFuture(new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.NEW,
                        configuration, creationTime, startingSegmentNumber));
            }

            return getCreationTime(context).thenCompose(storedCreationTime ->
                    zkStoreHelper.checkExists(configurationPath).thenCompose(configExists -> {
                        if (configExists) {
                            return handleConfigExists(storedCreationTime, startingSegmentNumber, 
                                    storedCreationTime == creationTime, context);
                        } else {
                            return CompletableFuture.completedFuture(new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.NEW,
                                    configuration, storedCreationTime, startingSegmentNumber));
                        }
                    }));
        });
    }

    @Override
    CompletableFuture<Void> createKVTableMetadata(OperationContext context) {
        return Futures.toVoid(zkStoreHelper.createZNodeIfNotExist(getKvtablePath()));
    }

    private CompletableFuture<CreateKVTableResponse> handleConfigExists(long creationTime, int startingSegmentNumber,
                                                                        boolean creationTimeMatched, OperationContext context) {
        CreateKVTableResponse.CreateStatus status = creationTimeMatched ?
                CreateKVTableResponse.CreateStatus.NEW : CreateKVTableResponse.CreateStatus.EXISTS_CREATING;

        return getConfiguration(context).thenCompose(config -> zkStoreHelper.checkExists(statePath)
                 .thenCompose(stateExists -> {
                     if (!stateExists) {
                         return CompletableFuture.completedFuture(new CreateKVTableResponse(status, 
                                 config, creationTime, startingSegmentNumber));
                     }

                     return getState(false, context).thenApply(state -> {
                         if (state.equals(KVTableState.UNKNOWN) || state.equals(KVTableState.CREATING)) {
                             return new CreateKVTableResponse(status, config, creationTime, startingSegmentNumber);
                         } else {
                             return new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.EXISTS_ACTIVE,
                                     config, creationTime, startingSegmentNumber);
                         }
                     });
                 }));
    }

    @Override
    public CompletableFuture<Long> getCreationTime(OperationContext context) {
        return getId(context).thenCompose(id -> zkStoreHelper.getCachedData(creationPath, id, x -> BitConverter.readLong(x, 0))
                .thenApply(VersionedMetadata::getObject));
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTEpochRecord>> getCurrentEpochRecordData(boolean ignoreCached, OperationContext context) {
        return getId(context).thenCompose(id -> {

            CompletableFuture<VersionedMetadata<Integer>> future;
            if (ignoreCached) {
                future = zkStoreHelper.getData(currentEpochRecordPath, x -> BitConverter.readInt(x, 0));
            } else {
                future = zkStoreHelper.getCachedData(currentEpochRecordPath, id, x -> BitConverter.readInt(x, 0));
            }
            return future.thenCompose(versionedEpochNumber -> getEpochRecord(versionedEpochNumber.getObject(), context)
                    .thenApply(epochRecord -> new VersionedMetadata<>(epochRecord, versionedEpochNumber.getVersion())));
        });
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTEpochRecord>> getEpochRecordData(int epoch, OperationContext context) {
        String path = String.format(epochRecordPathFormat, epoch);
        return getId(context).thenCompose(id -> zkStoreHelper.getCachedData(path, id, KVTEpochRecord::fromBytes));
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime, OperationContext context) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return Futures.toVoid(zkStoreHelper.createZNodeIfNotExist(creationPath, b));
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTConfigurationRecord>> getConfigurationData(boolean ignoreCached, OperationContext context) {
        return getId(context).thenCompose(id -> {
            if (ignoreCached) {
                return zkStoreHelper.getData(configurationPath, KVTConfigurationRecord::fromBytes);
            }

            return zkStoreHelper.getCachedData(configurationPath, id, KVTConfigurationRecord::fromBytes);
        });
    }

    @Override
    CompletableFuture<Void> createConfigurationIfAbsent(final KVTConfigurationRecord configuration, OperationContext context) {
        return Futures.toVoid(zkStoreHelper.createZNodeIfNotExist(configurationPath, configuration.toBytes()));
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(KVTEpochRecord data, OperationContext context) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, data.getEpoch());

        return Futures.toVoid(zkStoreHelper.createZNodeIfNotExist(currentEpochRecordPath, epochData));
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, KVTEpochRecord data, OperationContext context) {
        String path = String.format(epochRecordPathFormat, epoch);
        return Futures.toVoid(zkStoreHelper.createZNodeIfNotExist(path, data.toBytes()));
    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<KVTStateRecord> state, OperationContext context) {
        return getId(context).thenCompose(id -> zkStoreHelper.setData(statePath, state.getObject().toBytes(), state.getVersion())
                    .thenApply(r -> {
                        zkStoreHelper.invalidateCache(statePath, id);
                        return new Version.IntVersion(r);
                    }));
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTStateRecord>> getStateData(boolean ignoreCached, OperationContext context) {
        return getId(context).thenCompose(id -> {
            if (ignoreCached) {
                return zkStoreHelper.getData(statePath, KVTStateRecord::fromBytes);
            }

            return zkStoreHelper.getCachedData(statePath, id, KVTStateRecord::fromBytes);
        });
    }

    @Override
    public void refresh() {
        String id = this.idRef.getAndSet(null);
        id = id == null ? "" : id;
        // invalidate all mutable records in the cache 
        zkStoreHelper.invalidateCache(statePath, id);
        zkStoreHelper.invalidateCache(configurationPath, id);
        zkStoreHelper.invalidateCache(currentEpochRecordPath, id);
    }


    @Override
    public CompletableFuture<String> getId(OperationContext context) {
        String id = this.idRef.get();
        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            // We will return empty string as id if the UUID does not exist.
            // This can happen if stream is being created and we access the cache. 
            // Even if we access/load the cache against empty id, eventually cache will be populated against correct id 
            // once it is created. 
            return ZKScope.getKVTableInScopeZNodePath(this.scopeName, this.name)
                        .thenCompose(path -> zkStoreHelper.getData(path,
                                x -> BitConverter.readUUID(x, 0))
                        .thenApply(uuid -> {
                            String s = uuid.toString();
                            this.idRef.compareAndSet(null, s);
                            return s;
                        }));
            }
    }

    @Override
    public CompletableFuture<Void> delete(OperationContext context) {
        return zkStoreHelper.deleteTree(kvtablePath);
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent(KVTStateRecord state, OperationContext context) {
        return Futures.toVoid(zkStoreHelper.createZNodeIfNotExist(statePath, state.toBytes()));
    }
}
