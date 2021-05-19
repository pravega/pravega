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

import com.google.common.base.Preconditions;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.records.KVTConfigurationRecord;
import io.pravega.controller.store.kvtable.records.KVTEpochRecord;
import io.pravega.controller.store.kvtable.records.KVTStateRecord;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class InMemoryKVTable extends AbstractKVTableBase {
    private final UUID id;
    private final AtomicLong creationTime = new AtomicLong(Long.MIN_VALUE);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private VersionedMetadata<KVTConfigurationRecord> configuration;

    @GuardedBy("lock")
    private VersionedMetadata<KVTStateRecord> state;
    @GuardedBy("lock")
    private VersionedMetadata<KVTEpochRecord> currentEpochRecord;
    @GuardedBy("lock")
    private Map<Integer, VersionedMetadata<KVTEpochRecord>> epochRecords = new HashMap<>();

    public InMemoryKVTable(String scope, String name, UUID id) {
        super(scope, name);
        this.id = id;
    }

    public InMemoryKVTable(String scope, String name) {
        this(scope, name, UUID.randomUUID());
    }

    @Override
    public CompletableFuture<String> getId(OperationContext context) {
        return CompletableFuture.completedFuture(id.toString());
    }

    @Override
    public CompletableFuture<Void> delete(OperationContext context) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void refresh() {
    }

    @Override
    CompletableFuture<CreateKVTableResponse> checkKeyValueTableExists(KeyValueTableConfiguration configuration, 
            long timestamp, final int startingSegmentNumber, OperationContext context) {
        CompletableFuture<CreateKVTableResponse> result = new CompletableFuture<>();

        final long time;
        final KVTConfigurationRecord config;
        final VersionedMetadata<KVTStateRecord> currentState;
        synchronized (lock) {
            time = creationTime.get();
            config = this.configuration == null ? null : this.configuration.getObject();
            currentState = this.state;
        }

        if (time != Long.MIN_VALUE) {
            if (config != null) {
                handleMetadataExists(timestamp, result, time, startingSegmentNumber, config.getKvtConfiguration(), currentState);
            } else {
                result.complete(new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.NEW, configuration, time, startingSegmentNumber));
            }
        } else {
            result.complete(new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.NEW, configuration, timestamp, startingSegmentNumber));
        }

        return result;
    }

    @Override
    CompletableFuture<Void> createKVTableMetadata(OperationContext context) {
        return CompletableFuture.completedFuture(null);
    }

    private void handleMetadataExists(final long timestamp, CompletableFuture<CreateKVTableResponse> result, final long time,
                                      final int startingSegmentNumber, final KeyValueTableConfiguration config, VersionedMetadata<KVTStateRecord> currentState) {
        if (currentState != null) {
            KVTableState stateVal = currentState.getObject().getState();
            if (stateVal.equals(KVTableState.UNKNOWN) || stateVal.equals(KVTableState.CREATING)) {
                CreateKVTableResponse.CreateStatus status;
                status = (time == timestamp) ? CreateKVTableResponse.CreateStatus.NEW :
                        CreateKVTableResponse.CreateStatus.EXISTS_CREATING;
                result.complete(new CreateKVTableResponse(status, config, time, startingSegmentNumber));
            } else {
                result.complete(new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.EXISTS_ACTIVE, config, time, startingSegmentNumber));
            }
        } else {
            CreateKVTableResponse.CreateStatus status = (time == timestamp) ? CreateKVTableResponse.CreateStatus.NEW :
                    CreateKVTableResponse.CreateStatus.EXISTS_CREATING;

            result.complete(new CreateKVTableResponse(status, config, time, startingSegmentNumber));
        }
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(long timestamp, OperationContext context) {
        creationTime.compareAndSet(Long.MIN_VALUE, timestamp);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Long> getCreationTime(OperationContext context) {
        return CompletableFuture.completedFuture(creationTime.get());
    }

    @Override
    CompletableFuture<Void> createConfigurationIfAbsent(KVTConfigurationRecord config, OperationContext context) {
        Preconditions.checkNotNull(config);

        synchronized (lock) {
            if (configuration == null) {
                configuration = new VersionedMetadata<>(config, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTConfigurationRecord>> getConfigurationData(boolean ignoreCached, OperationContext context) {
        synchronized (lock) {
            if (this.configuration == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(this.configuration);
        }
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent(KVTStateRecord state, OperationContext context) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            if (this.state == null) {
                this.state = new VersionedMetadata<>(state, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> setStateData(VersionedMetadata<KVTStateRecord> newState, OperationContext context) {
        Preconditions.checkNotNull(newState);
        CompletableFuture<Version> result = new CompletableFuture<>();
        synchronized (lock) {
            if (Objects.equals(this.state.getVersion(), newState.getVersion())) {
                this.state = updatedCopy(newState);
                result.complete(this.state.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
            }
        }

        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTStateRecord>> getStateData(boolean ignoreCached, OperationContext context) {
        synchronized (lock) {
            if (this.state == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(state);
        }
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(KVTEpochRecord data, OperationContext context) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.currentEpochRecord == null) {
                this.currentEpochRecord = new VersionedMetadata<>(data, new Version.IntVersion(0));
            }
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTEpochRecord>> getCurrentEpochRecordData(boolean ignoreCached, OperationContext context) {
        synchronized (lock) {
            if (this.currentEpochRecord == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(this.currentEpochRecord);
        }
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, KVTEpochRecord data, OperationContext context) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            this.epochRecords.putIfAbsent(epoch, new VersionedMetadata<>(data, new Version.IntVersion(0)));
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTEpochRecord>> getEpochRecordData(int epoch, OperationContext context) {
        synchronized (lock) {
            if (!this.epochRecords.containsKey(epoch)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(this.epochRecords.get(epoch));
        }
    }

    private <T> VersionedMetadata<T> updatedCopy(VersionedMetadata<T> input) {
        return new VersionedMetadata<>(input.getObject(), new Version.IntVersion(input.getVersion().asIntVersion().getIntValue() + 1));
    }
}
