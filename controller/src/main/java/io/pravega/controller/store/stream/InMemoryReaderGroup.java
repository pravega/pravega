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
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;
import io.pravega.controller.store.stream.records.ReaderGroupStateRecord;

import java.util.concurrent.CompletableFuture;

public class InMemoryReaderGroup extends AbstractReaderGroup {
    InMemoryReaderGroup(String scopeName, String rgName) {
        super(scopeName, rgName);
    }

    @Override
    CompletableFuture<Void> createMetadataTables() {
        return null;
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(long creationTime) {
        return null;
    }

    @Override
    CompletableFuture<Void> createConfigurationIfAbsent(ReaderGroupConfig data) {
        return null;
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent() {
        return null;
    }

    @Override
    CompletableFuture<Version> setStateData(VersionedMetadata<ReaderGroupStateRecord> state) {
        return null;
    }

    @Override
    CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getConfigurationData(boolean ignoreCached) {
        return null;
    }

    @Override
    CompletableFuture<VersionedMetadata<ReaderGroupStateRecord>> getStateData(boolean ignoreCached) {
        return null;
    }

    @Override
    public CompletableFuture<Void> delete() {
        return null;
    }

    @Override
    public void refresh() {

    }
}
