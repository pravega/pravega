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
    public CompletableFuture<Void> create(ReaderGroupConfig configuration, long createTimestamp, OperationContext context) {
        return createMetadataTables(context)
                  .thenCompose((Void v) -> storeCreationTimeIfAbsent(createTimestamp, context))
                  .thenCompose((Void v) -> createConfigurationIfAbsent(configuration, context))
                  .thenCompose((Void v) -> createStateIfAbsent(context));
    }

    @Override
    public CompletableFuture<Void> startUpdateConfiguration(ReaderGroupConfig configuration, OperationContext context) {
        return getVersionedConfigurationRecord(context)
                .thenCompose(configRecord -> {
                    Preconditions.checkArgument(!configRecord.getObject().isUpdating());
                    Preconditions.checkArgument(configRecord.getObject().getGeneration() == configuration.getGeneration());
                    ReaderGroupConfigRecord update = ReaderGroupConfigRecord.update(configuration, configuration.getGeneration() + 1, true);
                    return Futures.toVoid(setConfigurationData(new VersionedMetadata<>(update, configRecord.getVersion()), context));
                });
    }

    @Override
    public CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<ReaderGroupConfigRecord> existing, OperationContext context) {
        Preconditions.checkNotNull(existing.getObject());
        if (existing.getObject().isUpdating()) {
            ReaderGroupConfigRecord updatedRecord = ReaderGroupConfigRecord.complete(existing.getObject());
            return Futures.toVoid(setConfigurationData(new VersionedMetadata<>(updatedRecord, existing.getVersion()), context));
        } else {
            // idempotent
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getVersionedConfigurationRecord(OperationContext context) {
        return getConfigurationData(true, context);
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupState>> getVersionedState(OperationContext context) {
        return getStateData(true, context)
                .thenApply(x -> new VersionedMetadata<>(x.getObject().getState(), x.getVersion()));
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupState>> updateVersionedState(VersionedMetadata<ReaderGroupState> previousState, 
                                                                                       ReaderGroupState newState, OperationContext context) {
        if (ReaderGroupState.isTransitionAllowed(previousState.getObject(), newState)) {
            return setStateData(new VersionedMetadata<>(ReaderGroupStateRecord.builder().state(newState).build(), previousState.getVersion()),
                    context)
                    .thenApply(updatedVersion -> new VersionedMetadata<>(newState, updatedVersion));
        } else {
            return Futures.failedFuture(StoreException.create(
                    StoreException.Type.OPERATION_NOT_ALLOWED,
                    "ReaderGroup: " + getName() + " State: " + newState.name() + " current state = " +
                            previousState.getObject()));
        }
    }

    @Override
    public CompletableFuture<ReaderGroupState> getState(boolean ignoreCached, OperationContext context) {
        return getStateData(ignoreCached, context)
                .thenApply(x -> x.getObject().getState());
    }

    abstract CompletableFuture<Void> createMetadataTables(OperationContext context);

    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime, OperationContext context);

    abstract CompletableFuture<Void> createConfigurationIfAbsent(final ReaderGroupConfig data, OperationContext context);

    abstract CompletableFuture<Void> createStateIfAbsent(OperationContext context);

    abstract CompletableFuture<Version> setStateData(final VersionedMetadata<ReaderGroupStateRecord> state,
                                                     OperationContext context);

    abstract CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getConfigurationData(boolean ignoreCached, 
                                                                                                OperationContext context);

    abstract CompletableFuture<VersionedMetadata<ReaderGroupStateRecord>> getStateData(boolean ignoreCached, 
                                                                                       OperationContext context);

    abstract CompletableFuture<Version> setConfigurationData(final VersionedMetadata<ReaderGroupConfigRecord> configuration, 
                                                             OperationContext context);
}
