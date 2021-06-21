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
package io.pravega.controller.store.stream;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;
import io.pravega.controller.store.stream.records.ReaderGroupStateRecord;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class InMemoryReaderGroup extends AbstractReaderGroup {
    private final AtomicLong creationTime = new AtomicLong(Long.MIN_VALUE);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private VersionedMetadata<ReaderGroupConfigRecord> configuration;
    @GuardedBy("lock")
    private VersionedMetadata<ReaderGroupStateRecord> state;
    private final UUID readerGroupId;

    public InMemoryReaderGroup(final String scopeName, final String rgName, final UUID rgId) {
        super(scopeName, rgName);
        this.readerGroupId = rgId;
    }

    public InMemoryReaderGroup(final String scopeName, final String rgName) {
        super(scopeName, rgName);
        this.readerGroupId = UUID.randomUUID();
    }

    public UUID getId() {
        return readerGroupId;
    }

    @Override
    CompletableFuture<Void> createMetadataTables(OperationContext context) {
        log.debug("InMemoryReaderGroup::createMetadataTables");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(long timestamp, OperationContext context) {
       creationTime.compareAndSet(Long.MIN_VALUE, timestamp);
        log.debug("InMemoryReaderGroup::storeCreationTimeIfAbsent");
       return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createConfigurationIfAbsent(ReaderGroupConfig config, OperationContext context) {
        Preconditions.checkNotNull(config);
        synchronized (lock) {
            if (configuration == null) {
                ReaderGroupConfigRecord configRecord = ReaderGroupConfigRecord.update(config, 0L, false);
                configuration = new VersionedMetadata<>(configRecord, new Version.IntVersion(0));
                log.debug("InMemoryReaderGroup::createConfigurationIfAbsent");
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent(OperationContext context) {
        synchronized (lock) {
            if (this.state == null) {
                ReaderGroupStateRecord stateRecord = ReaderGroupStateRecord.builder().state(ReaderGroupState.CREATING).build();
                this.state = new VersionedMetadata<>(stateRecord, new Version.IntVersion(0));
                log.debug("InMemoryReaderGroup::createStateIfAbsent");
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> setStateData(VersionedMetadata<ReaderGroupStateRecord> newState, OperationContext context) {
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
    CompletableFuture<VersionedMetadata<ReaderGroupStateRecord>> getStateData(boolean ignoreCached, OperationContext context) {
        log.debug("Inside getStateData - InMemoryStore");
        synchronized (lock) {
            if (this.state == null) {
                log.debug("stateData not found");
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            log.debug("returning stateData");
            return CompletableFuture.completedFuture(state);
        }
    }

    @Override
    CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getConfigurationData(boolean ignoreCached, OperationContext context) {
        synchronized (lock) {
            if (this.configuration == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(this.configuration);
        }
    }

    @Override
    CompletableFuture<Version> setConfigurationData(VersionedMetadata<ReaderGroupConfigRecord> newConfig, OperationContext context) {
        Preconditions.checkNotNull(newConfig);

        CompletableFuture<Version> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.configuration == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            } else {
                if (Objects.equals(this.configuration.getVersion(), newConfig.getVersion())) {
                    this.configuration = updatedCopy(new VersionedMetadata<>(newConfig.getObject(), this.configuration.getVersion()));
                    result.complete(this.configuration.getVersion());
                } else {
                    result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
                }
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> delete(OperationContext context) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void refresh() {
    }

    private <T> VersionedMetadata<T> updatedCopy(VersionedMetadata<T> input) {
        return new VersionedMetadata<>(input.getObject(), new Version.IntVersion(input.getVersion().asIntVersion().getIntValue() + 1));
    }
}
