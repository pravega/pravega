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
import java.util.concurrent.CompletableFuture;

@Slf4j
public abstract class AbstractReaderGroup implements ReaderGroup {

    private final String scope;
    private final String name;

    AbstractReaderGroup(String scopeName, String rgName) {
        this.scope = scopeName;
        this.name = rgName;
    }

    @Override
    public String getScope() {
        return scope;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CompletableFuture<Void> create(ReaderGroupConfig configuration, long createTimestamp) {
        return createMetadataTables()
                  .thenCompose((Void v) -> storeCreationTimeIfAbsent(createTimestamp))
                  .thenCompose((Void v) -> createConfigurationIfAbsent(configuration))
                  .thenCompose((Void v) -> createStateIfAbsent());
    }

    @Override
    public CompletableFuture<Void> startUpdateConfiguration(ReaderGroupConfig configuration) {
        return getVersionedConfigurationRecord()
                .thenCompose(configRecord -> {
                    Preconditions.checkArgument(!configRecord.getObject().isUpdating());
                    Preconditions.checkArgument(configRecord.getObject().getGeneration() == configuration.getGeneration());
                    ReaderGroupConfigRecord update = ReaderGroupConfigRecord.update(configuration, configuration.getGeneration() + 1, true);
                    return Futures.toVoid(setConfigurationData(new VersionedMetadata<>(update, configRecord.getVersion())));
                });
    }

    @Override
    public CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<ReaderGroupConfigRecord> existing) {
        Preconditions.checkNotNull(existing.getObject());
        if (existing.getObject().isUpdating()) {
            ReaderGroupConfigRecord updatedRecord = ReaderGroupConfigRecord.complete(existing.getObject());
            return Futures.toVoid(setConfigurationData(new VersionedMetadata<>(updatedRecord, existing.getVersion())));
        } else {
            // idempotent
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getVersionedConfigurationRecord() {
        return getConfigurationData(true);
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupState>> getVersionedState() {
        return getStateData(true)
                .thenApply(x -> new VersionedMetadata<>(x.getObject().getState(), x.getVersion()));
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupState>> updateVersionedState(VersionedMetadata<ReaderGroupState> previousState, ReaderGroupState newState) {
        if (ReaderGroupState.isTransitionAllowed(previousState.getObject(), newState)) {
            return setStateData(new VersionedMetadata<>(ReaderGroupStateRecord.builder().state(newState).build(), previousState.getVersion()))
                    .thenApply(updatedVersion -> new VersionedMetadata<>(newState, updatedVersion));
        } else {
            return Futures.failedFuture(StoreException.create(
                    StoreException.Type.OPERATION_NOT_ALLOWED,
                    "ReaderGroup: " + getName() + " State: " + newState.name() + " current state = " +
                            previousState.getObject()));
        }
    }

    @Override
    public CompletableFuture<ReaderGroupState> getState(boolean ignoreCached) {
        return getStateData(ignoreCached)
                .thenApply(x -> x.getObject().getState());
    }

    abstract CompletableFuture<Void> createMetadataTables();

    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime);

    abstract CompletableFuture<Void> createConfigurationIfAbsent(final ReaderGroupConfig data);

    abstract CompletableFuture<Void> createStateIfAbsent();

    abstract CompletableFuture<Version> setStateData(final VersionedMetadata<ReaderGroupStateRecord> state);

    abstract CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getConfigurationData(boolean ignoreCached);

    abstract CompletableFuture<VersionedMetadata<ReaderGroupStateRecord>> getStateData(boolean ignoreCached);

    abstract CompletableFuture<Version> setConfigurationData(final VersionedMetadata<ReaderGroupConfigRecord> configuration);
}
