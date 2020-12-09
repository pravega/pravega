/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;
import io.pravega.controller.store.stream.records.ReaderGroupStateRecord;
import io.pravega.shared.NameUtils;

import java.util.concurrent.CompletableFuture;

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
    public String getScopedName() {
        return NameUtils.getScopedReaderGroupName(this.scope, this.name);
    }

    @Override
    public CompletableFuture<Void> create(ReaderGroupConfig configuration, long createTimestamp) {
        return createMetadataTables()
                  .thenCompose((Void v) -> storeCreationTimeIfAbsent(createTimestamp))
                  .thenCompose((Void v) -> createConfigurationIfAbsent(configuration))
                  .thenCompose((Void v) -> createStateIfAbsent());
    }

    @Override
    public CompletableFuture<Void> delete() {
        return null;
    }

    @Override
    public CompletableFuture<Long> getCreationTime() {
        return null;
    }

    @Override
    public CompletableFuture<Void> startUpdateConfiguration(ReaderGroupConfig configuration) {
        return null;
    }

    @Override
    public CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<ReaderGroupConfig> existing) {
        return null;
    }

    @Override
    public CompletableFuture<ReaderGroupConfig> getConfiguration() {
        return null;
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
    public CompletableFuture<Void> updateState(ReaderGroupState state) {
        return null;
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


}
